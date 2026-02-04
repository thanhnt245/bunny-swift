// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin
//
// Portions derived from PostgresNIO (licensed under the MIT License)
// Copyright (c) 2017-2024 Vapor

import Foundation

@usableFromInline
struct PoolStateMachine<
  Connection: PoolableConnection,
  ConnectionIDGenerator: ConnectionIDGeneratorProtocol,
  Request: ConnectionRequestProtocol,
  TimerCancellationToken: Sendable & Hashable
>: Sendable
where
  Connection.ID == ConnectionIDGenerator.ID,
  Request.Connection == Connection
{
  @usableFromInline
  typealias ConnectionID = Connection.ID

  @usableFromInline
  typealias RequestID = Request.ID

  // MARK: - Configuration

  @usableFromInline
  struct Configuration: Sendable {
    @usableFromInline var minimumConnectionCount: Int
    @usableFromInline var maximumConnectionSoftLimit: Int
    @usableFromInline var maximumConnectionHardLimit: Int
    @usableFromInline var keepAliveFrequency: Duration?
    @usableFromInline var idleTimeout: Duration
    @usableFromInline var circuitBreakerTripAfter: Duration

    @inlinable
    init(
      minimumConnectionCount: Int,
      maximumConnectionSoftLimit: Int,
      maximumConnectionHardLimit: Int,
      keepAliveFrequency: Duration?,
      idleTimeout: Duration,
      circuitBreakerTripAfter: Duration
    ) {
      self.minimumConnectionCount = minimumConnectionCount
      self.maximumConnectionSoftLimit = maximumConnectionSoftLimit
      self.maximumConnectionHardLimit = maximumConnectionHardLimit
      self.keepAliveFrequency = keepAliveFrequency
      self.idleTimeout = idleTimeout
      self.circuitBreakerTripAfter = circuitBreakerTripAfter
    }
  }

  // MARK: - Timer

  @usableFromInline
  struct Timer: Hashable, Sendable {
    @usableFromInline
    enum UseCase: Hashable, Sendable {
      case backoff
      case keepAlive
      case idleTimeout
    }

    @usableFromInline let timerID: Int
    @usableFromInline let connectionID: ConnectionID?
    @usableFromInline let duration: Duration
    @usableFromInline let useCase: UseCase

    @inlinable
    init(timerID: Int, connectionID: ConnectionID?, duration: Duration, useCase: UseCase) {
      self.timerID = timerID
      self.connectionID = connectionID
      self.duration = duration
      self.useCase = useCase
    }
  }

  // MARK: - Connection State

  @usableFromInline
  enum ConnectionState: Sendable {
    case idle(
      Connection, idleTimerToken: TimerCancellationToken?,
      keepAliveTimerToken: TimerCancellationToken?)
    case leased(Connection)
    case closing(Connection)
  }

  // MARK: - Pool State

  @usableFromInline
  enum PoolState: Sendable {
    case running
    case connectionCreationFailing(FailingContext)
    case circuitBreakerOpen(FailingContext)
    case shuttingDown
    case shutDown

    @usableFromInline
    struct FailingContext: Sendable {
      @usableFromInline var timeOfFirstFailure: ContinuousClock.Instant
      @usableFromInline var numberOfFailedAttempts: Int
      @usableFromInline var lastError: any Error
      @usableFromInline var retryConnectionID: ConnectionID?

      @inlinable
      init(
        timeOfFirstFailure: ContinuousClock.Instant,
        numberOfFailedAttempts: Int,
        lastError: any Error,
        retryConnectionID: ConnectionID?
      ) {
        self.timeOfFirstFailure = timeOfFirstFailure
        self.numberOfFailedAttempts = numberOfFailedAttempts
        self.lastError = lastError
        self.retryConnectionID = retryConnectionID
      }
    }
  }

  // MARK: - Actions

  @usableFromInline
  struct Action: Sendable {
    @usableFromInline var request: RequestAction
    @usableFromInline var connection: ConnectionAction

    @inlinable
    init(request: RequestAction, connection: ConnectionAction) {
      self.request = request
      self.connection = connection
    }

    @inlinable
    static func none() -> Action {
      Action(request: .none, connection: .none)
    }
  }

  @usableFromInline
  enum RequestAction: Sendable {
    case leaseConnection(Request, Connection)
    case queueRequest(Request)
    case failRequest(Request, ConnectionPoolError)
    case failRequests(TinyFastSequence<Request>, ConnectionPoolError)
    case none
  }

  @usableFromInline
  enum ConnectionAction: Sendable {
    case scheduleTimers(Max2Sequence<Timer>)
    case createConnection(ConnectionID)
    case runKeepAlive(Connection)
    case closeConnection(Connection, Max2Sequence<TimerCancellationToken>)
    case shutdown(ShutdownContext)
    case cancelTimers(TinyFastSequence<TimerCancellationToken>)
    case none

    @usableFromInline
    struct ShutdownContext: Sendable {
      @usableFromInline var connections: [Connection] = []
      @usableFromInline var timersToCancel: [TimerCancellationToken] = []

      @inlinable
      init() {}
    }
  }

  // MARK: - State

  @usableFromInline let configuration: Configuration
  @usableFromInline let idGenerator: ConnectionIDGenerator
  @usableFromInline var poolState: PoolState = .running
  @usableFromInline var connections: [ConnectionID: ConnectionState] = [:]
  @usableFromInline var requestQueue = RequestQueue<Request>()
  @usableFromInline var timerIDCounter = 0
  @usableFromInline var timerCancellations: [Int: TimerCancellationToken] = [:]
  @usableFromInline var pendingConnectionCount = 0
  @usableFromInline let clock = ContinuousClock()

  // MARK: - Initialization

  @inlinable
  init(configuration: Configuration, idGenerator: ConnectionIDGenerator) {
    self.configuration = configuration
    self.idGenerator = idGenerator
  }

  // MARK: - Lease Connection

  @inlinable
  mutating func leaseConnection(_ request: Request) -> Action {
    switch poolState {
    case .running, .connectionCreationFailing:
      return handleLeaseRequest(request)
    case .circuitBreakerOpen:
      return Action(request: .failRequest(request, .circuitBreakerTripped), connection: .none)
    case .shuttingDown, .shutDown:
      return Action(request: .failRequest(request, .poolShutdown), connection: .none)
    }
  }

  @inlinable
  mutating func handleLeaseRequest(_ request: Request) -> Action {
    // Try to find an idle connection
    for (id, state) in connections {
      if case .idle(let conn, let idleTimer, let keepAliveTimer) = state {
        connections[id] = .leased(conn)
        var timersToCancel = TinyFastSequence<TimerCancellationToken>()
        if let token = idleTimer { timersToCancel.append(token) }
        if let token = keepAliveTimer { timersToCancel.append(token) }
        return Action(
          request: .leaseConnection(request, conn),
          connection: timersToCancel.isEmpty ? .none : .cancelTimers(timersToCancel)
        )
      }
    }

    // No idle connection, queue the request
    requestQueue.enqueue(request)

    // Try to create a new connection if under limits
    let totalConnections = connections.count + pendingConnectionCount
    if totalConnections < configuration.maximumConnectionHardLimit {
      let createAction = createNewConnection()
      return Action(request: .queueRequest(request), connection: createAction.connection)
    }

    return Action(request: .queueRequest(request), connection: .none)
  }

  @inlinable
  mutating func createNewConnection() -> Action {
    let connectionID = idGenerator.next()
    pendingConnectionCount += 1
    return Action(request: .none, connection: .createConnection(connectionID))
  }

  // MARK: - Release Connection

  @inlinable
  mutating func releaseConnection(_ connection: Connection) -> Action {
    let id = connection.id
    guard let state = connections[id] else { return .none() }

    switch state {
    case .leased:
      return handleConnectionBecameAvailable(connection)
    case .idle, .closing:
      return .none()
    }
  }

  @inlinable
  mutating func handleConnectionBecameAvailable(_ connection: Connection) -> Action {
    // Service queued requests
    if let request = requestQueue.popFirst() {
      connections[connection.id] = .leased(connection)
      return Action(request: .leaseConnection(request, connection), connection: .none)
    }

    // Park the connection
    return parkConnection(connection)
  }

  @inlinable
  mutating func parkConnection(_ connection: Connection) -> Action {
    let totalConnections = connections.count

    // Close if above soft limit
    if totalConnections > configuration.maximumConnectionSoftLimit {
      connections[connection.id] = .closing(connection)
      return Action(request: .none, connection: .closeConnection(connection, Max2Sequence()))
    }

    // Schedule idle timeout and optional keep-alive
    var timers = Max2Sequence<Timer>()

    let idleTimer = makeTimer(
      for: connection.id, duration: configuration.idleTimeout, useCase: .idleTimeout)
    timers.append(idleTimer)

    if let keepAliveFrequency = configuration.keepAliveFrequency {
      let keepAliveTimer = makeTimer(
        for: connection.id, duration: keepAliveFrequency, useCase: .keepAlive)
      timers.append(keepAliveTimer)
    }

    connections[connection.id] = .idle(connection, idleTimerToken: nil, keepAliveTimerToken: nil)
    return Action(request: .none, connection: .scheduleTimers(timers))
  }

  // MARK: - Connection Lifecycle

  @inlinable
  mutating func connectionEstablished(_ connection: Connection) -> Action {
    pendingConnectionCount = max(0, pendingConnectionCount - 1)
    connections[connection.id] = .leased(connection)

    // Reset pool state on successful connection
    switch poolState {
    case .connectionCreationFailing, .circuitBreakerOpen:
      poolState = .running
    case .running, .shuttingDown, .shutDown:
      break
    }

    return handleConnectionBecameAvailable(connection)
  }

  @inlinable
  mutating func connectionCreationFailed(_ connectionID: ConnectionID, error: any Error) -> Action {
    pendingConnectionCount = max(0, pendingConnectionCount - 1)

    switch poolState {
    case .running:
      let context = PoolState.FailingContext(
        timeOfFirstFailure: clock.now,
        numberOfFailedAttempts: 1,
        lastError: error,
        retryConnectionID: connectionID
      )
      poolState = .connectionCreationFailing(context)
      return scheduleBackoffTimer(attempts: 1)

    case .connectionCreationFailing(var context):
      context.numberOfFailedAttempts += 1
      context.lastError = error
      context.retryConnectionID = connectionID

      // Check if circuit breaker should trip
      let elapsed = clock.now - context.timeOfFirstFailure
      if elapsed >= configuration.circuitBreakerTripAfter && connections.isEmpty {
        poolState = .circuitBreakerOpen(context)
        let failedRequests = TinyFastSequence(contentsOf: requestQueue.removeAll())
        return Action(
          request: .failRequests(failedRequests, .circuitBreakerTripped),
          connection: scheduleBackoffTimer(attempts: context.numberOfFailedAttempts).connection
        )
      }

      poolState = .connectionCreationFailing(context)
      return scheduleBackoffTimer(attempts: context.numberOfFailedAttempts)

    case .circuitBreakerOpen(var context):
      context.numberOfFailedAttempts += 1
      context.lastError = error
      context.retryConnectionID = connectionID
      poolState = .circuitBreakerOpen(context)
      return scheduleBackoffTimer(attempts: context.numberOfFailedAttempts)

    case .shuttingDown, .shutDown:
      return .none()
    }
  }

  @inlinable
  mutating func connectionClosed(_ connectionID: ConnectionID) -> Action {
    guard let state = connections.removeValue(forKey: connectionID) else {
      return .none()
    }

    var timersToCancel = Max2Sequence<TimerCancellationToken>()
    if case .idle(_, let idleTimer, let keepAliveTimer) = state {
      if let token = idleTimer { timersToCancel.append(token) }
      if let token = keepAliveTimer { timersToCancel.append(token) }
    }

    // Check if pool should fully shut down
    if case .shuttingDown = poolState, connections.isEmpty, pendingConnectionCount == 0 {
      poolState = .shutDown
    }

    // Refill connections if needed
    if case .running = poolState {
      let totalConnections = connections.count + pendingConnectionCount
      if totalConnections < configuration.minimumConnectionCount && !requestQueue.isEmpty {
        let createAction = createNewConnection()
        if timersToCancel.isEmpty {
          return createAction
        }
        return Action(request: .none, connection: createAction.connection)
      }
    }

    if timersToCancel.isEmpty {
      return .none()
    }
    return Action(
      request: .none, connection: .cancelTimers(TinyFastSequence(contentsOf: timersToCancel)))
  }

  // MARK: - Timer Handling

  @inlinable
  mutating func makeTimer(
    for connectionID: ConnectionID?, duration: Duration, useCase: Timer.UseCase
  ) -> Timer {
    timerIDCounter += 1
    return Timer(
      timerID: timerIDCounter, connectionID: connectionID, duration: duration, useCase: useCase)
  }

  @inlinable
  mutating func timerScheduled(_ timer: Timer, cancellationToken: TimerCancellationToken) {
    timerCancellations[timer.timerID] = cancellationToken

    if let connectionID = timer.connectionID {
      if case .idle(let conn, var idleToken, var keepAliveToken) = connections[connectionID] {
        switch timer.useCase {
        case .idleTimeout:
          idleToken = cancellationToken
        case .keepAlive:
          keepAliveToken = cancellationToken
        case .backoff:
          break
        }
        connections[connectionID] = .idle(
          conn, idleTimerToken: idleToken, keepAliveTimerToken: keepAliveToken)
      }
    }
  }

  @inlinable
  mutating func timerTriggered(_ timer: Timer) -> Action {
    timerCancellations.removeValue(forKey: timer.timerID)

    switch timer.useCase {
    case .idleTimeout:
      return handleIdleTimeout(timer)
    case .keepAlive:
      return handleKeepAliveTimer(timer)
    case .backoff:
      return handleBackoffTimer(timer)
    }
  }

  @inlinable
  mutating func handleIdleTimeout(_ timer: Timer) -> Action {
    guard let connectionID = timer.connectionID,
      case .idle(let conn, _, let keepAliveToken) = connections[connectionID]
    else {
      return .none()
    }

    connections[connectionID] = .closing(conn)
    var timersToCancel = Max2Sequence<TimerCancellationToken>()
    if let token = keepAliveToken { timersToCancel.append(token) }
    return Action(request: .none, connection: .closeConnection(conn, timersToCancel))
  }

  @inlinable
  mutating func handleKeepAliveTimer(_ timer: Timer) -> Action {
    guard let connectionID = timer.connectionID,
      case .idle(let conn, _, _) = connections[connectionID]
    else {
      return .none()
    }

    return Action(request: .none, connection: .runKeepAlive(conn))
  }

  @inlinable
  mutating func handleBackoffTimer(_ timer: Timer) -> Action {
    let connectionID: ConnectionID?
    switch poolState {
    case .connectionCreationFailing(let context):
      connectionID = context.retryConnectionID
    case .circuitBreakerOpen(let context):
      connectionID = context.retryConnectionID
    default:
      return .none()
    }

    guard let connectionID = connectionID else {
      return .none()
    }

    pendingConnectionCount += 1
    return Action(request: .none, connection: .createConnection(connectionID))
  }

  @inlinable
  mutating func scheduleBackoffTimer(attempts: Int) -> Action {
    let backoff = Self.calculateBackoff(failedAttempt: attempts)
    let timer = makeTimer(for: nil, duration: backoff, useCase: .backoff)
    return Action(request: .none, connection: .scheduleTimers(Max2Sequence(timer)))
  }

  // MARK: - Keep-Alive Results

  @inlinable
  mutating func keepAliveSucceeded(_ connection: Connection) -> Action {
    guard case .idle = connections[connection.id] else {
      return .none()
    }
    return parkConnection(connection)
  }

  @inlinable
  mutating func keepAliveFailed(_ connectionID: ConnectionID) -> Action {
    guard case .idle(let conn, let idleToken, let keepAliveToken) = connections[connectionID] else {
      return .none()
    }

    connections[connectionID] = .closing(conn)
    var timersToCancel = Max2Sequence<TimerCancellationToken>()
    if let token = idleToken { timersToCancel.append(token) }
    if let token = keepAliveToken { timersToCancel.append(token) }
    return Action(request: .none, connection: .closeConnection(conn, timersToCancel))
  }

  // MARK: - Shutdown

  @inlinable
  mutating func triggerShutdown() -> Action {
    switch poolState {
    case .running, .connectionCreationFailing, .circuitBreakerOpen:
      poolState = .shuttingDown

      var shutdown = ConnectionAction.ShutdownContext()
      for (id, state) in connections {
        switch state {
        case .idle(let conn, let idleToken, let keepAliveToken):
          shutdown.connections.append(conn)
          if let token = idleToken { shutdown.timersToCancel.append(token) }
          if let token = keepAliveToken { shutdown.timersToCancel.append(token) }
          connections[id] = .closing(conn)
        case .leased(let conn):
          shutdown.connections.append(conn)
          connections[id] = .closing(conn)
        case .closing:
          break
        }
      }

      let failedRequests = TinyFastSequence(contentsOf: requestQueue.removeAll())

      if connections.isEmpty && pendingConnectionCount == 0 {
        poolState = .shutDown
      }

      return Action(
        request: failedRequests.isEmpty ? .none : .failRequests(failedRequests, .poolShutdown),
        connection: .shutdown(shutdown)
      )

    case .shuttingDown, .shutDown:
      return .none()
    }
  }

  @inlinable
  mutating func cancelRequest(_ requestID: RequestID) -> Action {
    guard let request = requestQueue.cancel(requestID) else {
      return .none()
    }
    return Action(request: .failRequest(request, .requestCancelled), connection: .none)
  }

  // MARK: - Backoff Calculation

  @inlinable
  static func calculateBackoff(failedAttempt attempts: Int) -> Duration {
    let baseNanoseconds: Double = 100_000_000
    let backoffNanoseconds = baseNanoseconds * pow(1.25, Double(attempts - 1))
    let cappedNanoseconds = min(backoffNanoseconds, 60_000_000_000)

    let jitterRange = Int64(cappedNanoseconds / 100) * 3
    let jitter = Int64.random(in: -jitterRange...jitterRange)

    return Duration.nanoseconds(Int64(cappedNanoseconds) + jitter)
  }
}
