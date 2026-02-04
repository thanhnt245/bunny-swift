// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import Atomics
import Foundation

/// Global ID allocator for pooled connections
private let pooledConnectionIDAllocator = ManagedAtomic<Int>(0)

/// A wrapper that adds pool-compatible semantics to any async-closeable connection.
///
/// This wrapper adds:
/// - An `id` property for pool tracking (not exposed to users of the underlying connection)
/// - Synchronous `close()` that triggers async cleanup
/// - Thread-safe `onClose` handler registration
///
/// Users interact with the underlying connection directly; the wrapper handles pool lifecycle.
public final class PooledConnection<Wrapped: Sendable>: PoolableConnection, @unchecked Sendable {
  public typealias ID = Int

  public let id: ID
  public let connection: Wrapped

  private let closeAction: @Sendable (Wrapped) -> Void
  private let onCloseHandlers = LockedValueBox<[@Sendable ((any Error)?) -> Void]>([])

  /// Creates a wrapper around an existing connection with an auto-generated ID.
  ///
  /// - Parameters:
  ///   - connection: The underlying connection
  ///   - closeAction: Called when the pool requests connection closure
  public init(
    connection: Wrapped,
    closeAction: @escaping @Sendable (Wrapped) -> Void
  ) {
    self.id = pooledConnectionIDAllocator.loadThenWrappingIncrement(ordering: .relaxed)
    self.connection = connection
    self.closeAction = closeAction
  }

  /// Creates a wrapper around an existing connection with a specific ID.
  /// Use this when the pool provides the connection ID.
  ///
  /// - Parameters:
  ///   - id: The pool-assigned identifier
  ///   - connection: The underlying connection
  ///   - closeAction: Called when the pool requests connection closure
  public init(
    id: ID,
    connection: Wrapped,
    closeAction: @escaping @Sendable (Wrapped) -> Void
  ) {
    self.id = id
    self.connection = connection
    self.closeAction = closeAction
  }

  public func onClose(_ closure: @escaping @Sendable ((any Error)?) -> Void) {
    onCloseHandlers.withLockedValue { $0.append(closure) }
  }

  public func close() {
    closeAction(connection)
    onCloseHandlers.withLockedValue { handlers in
      for handler in handlers {
        handler(nil as (any Error)?)
      }
    }
  }

  /// Notifies handlers that the connection closed with an error.
  /// Call this if the underlying connection closes unexpectedly.
  public func notifyClose(error: (any Error)?) {
    onCloseHandlers.withLockedValue { handlers in
      for handler in handlers {
        handler(error)
      }
    }
  }
}
