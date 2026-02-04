// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import ConnectionPool

/// A connection pool for AMQP connections.
///
/// Manages a pool of reusable connections with configurable size and idle timeout.
///
/// ```swift
/// let pool = BunnyConnectionPool(size: 4)
///
/// try await pool.withConnection { connection in
///     try await connection.withChannel { channel in
///         let queue = try await channel.queue("hello")
///         try await channel.basicPublish(body: data, routingKey: queue.name)
///     }
/// }
/// ```
public final class BunnyConnectionPool: Sendable {
  private typealias WrappedConnection = PooledConnection<Connection>

  private typealias InnerPool = GenericConnectionPool<
    WrappedConnection,
    ConnectionIDGenerator,
    NoKeepAliveBehavior<WrappedConnection>,
    NoOpConnectionPoolObservabilityDelegate<Int>
  >

  private let inner: InnerPool
  private let runTask: Task<Void, Never>

  /// Creates a new connection pool.
  ///
  /// - Parameters:
  ///   - configuration: Connection settings (host, port, credentials, etc.)
  ///   - size: Maximum number of connections in the pool (default: 4)
  ///   - idleTimeout: How long idle connections stay open (default: 60 minutes)
  public init(
    configuration: ConnectionConfiguration = ConnectionConfiguration(),
    size: Int = 4,
    idleTimeout: Duration = .seconds(3600)
  ) {
    precondition(size > 0, "Pool size must be at least 1")

    let poolConfig = ConnectionPoolConfiguration(
      minimumConnectionCount: 0,
      maximumConnectionSoftLimit: size,
      maximumConnectionHardLimit: size,
      idleTimeout: idleTimeout
    )

    self.inner = InnerPool(
      configuration: poolConfig,
      idGenerator: ConnectionIDGenerator(),
      keepAliveBehavior: NoKeepAliveBehavior<WrappedConnection>(),
      observabilityDelegate: NoOpConnectionPoolObservabilityDelegate<Int>()
    ) { (id: Int) in
      let connection = try await Connection.open(configuration)
      return PooledConnection(id: id, connection: connection) { conn in
        Task { try? await conn.close() }
      }
    }

    let pool = self.inner
    self.runTask = Task { await pool.run() }
  }

  /// Creates a new connection pool with URI.
  ///
  /// - Parameters:
  ///   - uri: AMQP URI (e.g., "amqp://guest:guest@localhost:5672/")
  ///   - size: Maximum number of connections in the pool (default: 4)
  ///   - idleTimeout: How long idle connections stay open (default: 60 minutes)
  public convenience init(
    uri: String,
    size: Int = 4,
    idleTimeout: Duration = .seconds(3600)
  ) throws {
    let configuration = try ConnectionConfiguration.from(uri: uri)
    self.init(configuration: configuration, size: size, idleTimeout: idleTimeout)
  }

  deinit {
    inner.shutdown()
    runTask.cancel()
  }

  /// Executes a closure with a leased connection.
  ///
  /// The connection is automatically returned to the pool when the closure completes.
  ///
  /// - SeeAlso: [Connection churn](https://www.rabbitmq.com/docs/connections#high-connection-churn)
  public func withConnection<T: Sendable>(
    _ body: @Sendable (Connection) async throws -> T
  ) async throws -> T {
    let lease = try await inner.leaseConnection()
    defer { lease.release() }
    return try await body(lease.connection.connection)
  }

  /// Shuts down the pool, closing all connections.
  public func shutdown() {
    inner.shutdown()
    runTask.cancel()
  }
}
