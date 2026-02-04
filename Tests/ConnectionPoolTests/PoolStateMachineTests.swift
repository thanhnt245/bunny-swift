// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import Testing

@testable import ConnectionPool

// MARK: - Test Fixtures

final class MockConnection: PooledConnection, @unchecked Sendable {
    let id: Int
    private var closeHandlers: [@Sendable ((any Error)?) -> Void] = []
    var isClosed = false

    init(id: Int) {
        self.id = id
    }

    func onClose(_ closure: @escaping @Sendable ((any Error)?) -> Void) {
        closeHandlers.append(closure)
    }

    func close() {
        isClosed = true
        for handler in closeHandlers {
            handler(nil)
        }
    }

    func simulateClose(error: (any Error)? = nil) {
        for handler in closeHandlers {
            handler(error)
        }
    }
}

struct MockRequest: ConnectionRequestProtocol {
    typealias ID = Int
    typealias Connection = MockConnection

    let id: Int
    var completionResult: Result<ConnectionLease<MockConnection>, ConnectionPoolError>?

    func complete(with result: Result<ConnectionLease<MockConnection>, ConnectionPoolError>) {
        // In tests, we just track the result
    }
}

struct MockTimerToken: Hashable, Sendable {
    let id: Int
}

typealias TestStateMachine = PoolStateMachine<MockConnection, ConnectionIDGenerator, MockRequest, MockTimerToken>

func makeStateMachine(
    minimumConnections: Int = 0,
    softLimit: Int = 4,
    hardLimit: Int = 4,
    idleTimeout: Duration = .seconds(60)
) -> TestStateMachine {
    let config = TestStateMachine.Configuration(
        minimumConnectionCount: minimumConnections,
        maximumConnectionSoftLimit: softLimit,
        maximumConnectionHardLimit: hardLimit,
        keepAliveFrequency: nil,
        idleTimeout: idleTimeout,
        circuitBreakerTripAfter: .seconds(15)
    )
    return TestStateMachine(configuration: config, idGenerator: ConnectionIDGenerator())
}

// MARK: - Tests

@Suite("Pool State Machine Tests")
struct PoolStateMachineTests {

    @Test("Lease request with no connections creates a new connection")
    func leaseCreatesConnection() {
        var sm = makeStateMachine()
        let request = MockRequest(id: 1)

        let action = sm.leaseConnection(request)

        if case .createConnection(let id) = action.connection {
            #expect(id >= 0)
        } else {
            Issue.record("Expected createConnection action")
        }
    }

    @Test("Lease request reuses idle connection")
    func leaseReusesIdleConnection() {
        var sm = makeStateMachine()
        let conn = MockConnection(id: 1)

        _ = sm.connectionEstablished(conn)
        _ = sm.releaseConnection(conn)

        let request = MockRequest(id: 2)
        let action = sm.leaseConnection(request)

        if case .leaseConnection(let req, let leasedConn) = action.request {
            #expect(req.id == 2)
            #expect(leasedConn.id == conn.id)
        } else {
            Issue.record("Expected leaseConnection action")
        }
    }

    @Test("Multiple requests are queued when at connection limit")
    func requestsQueuedAtLimit() {
        var sm = makeStateMachine(hardLimit: 1)
        let conn = MockConnection(id: 0)

        _ = sm.connectionEstablished(conn)

        let request1 = MockRequest(id: 1)
        let request2 = MockRequest(id: 2)

        let action1 = sm.leaseConnection(request1)
        let action2 = sm.leaseConnection(request2)

        if case .leaseConnection = action1.request {
            // First request gets the connection
        } else {
            Issue.record("First request should get connection")
        }

        // Second request should be queued (no connection action since at limit)
        if case .none = action2.connection {
            // Expected - no new connection created
        } else {
            Issue.record("Should not create connection when at limit")
        }
    }

    @Test("Released connection services queued request")
    func releasedConnectionServicesQueue() {
        var sm = makeStateMachine(hardLimit: 1)
        let conn = MockConnection(id: 0)

        _ = sm.connectionEstablished(conn)

        let request1 = MockRequest(id: 1)
        let request2 = MockRequest(id: 2)

        _ = sm.leaseConnection(request1)
        _ = sm.leaseConnection(request2)

        let releaseAction = sm.releaseConnection(conn)

        if case .leaseConnection(let req, _) = releaseAction.request {
            #expect(req.id == 2)
        } else {
            Issue.record("Should lease to queued request")
        }
    }

    @Test("Connection closed removes from pool")
    func connectionClosedRemovesFromPool() {
        var sm = makeStateMachine()
        let conn = MockConnection(id: 0)

        _ = sm.connectionEstablished(conn)
        _ = sm.releaseConnection(conn)

        let action = sm.connectionClosed(0)

        // Should not crash and return some action
        if case .none = action.request {
            // Expected
        }
    }

    @Test("Shutdown fails pending requests")
    func shutdownFailsPendingRequests() {
        var sm = makeStateMachine(hardLimit: 1)
        let conn = MockConnection(id: 0)

        _ = sm.connectionEstablished(conn)

        let request1 = MockRequest(id: 1)
        let request2 = MockRequest(id: 2)

        _ = sm.leaseConnection(request1)
        _ = sm.leaseConnection(request2)

        let shutdownAction = sm.triggerShutdown()

        if case .failRequests(let requests, let error) = shutdownAction.request {
            #expect(requests.count == 1)
            #expect(error == .poolShutdown)
        } else {
            Issue.record("Should fail queued requests")
        }
    }

    @Test("Shutdown prevents new leases")
    func shutdownPreventsNewLeases() {
        var sm = makeStateMachine()

        _ = sm.triggerShutdown()

        let request = MockRequest(id: 1)
        let action = sm.leaseConnection(request)

        if case .failRequest(_, let error) = action.request {
            #expect(error == .poolShutdown)
        } else {
            Issue.record("Should fail request after shutdown")
        }
    }

    @Test("Cancel request removes from queue")
    func cancelRequestRemovesFromQueue() {
        var sm = makeStateMachine(hardLimit: 1)
        let conn = MockConnection(id: 0)

        _ = sm.connectionEstablished(conn)

        let request1 = MockRequest(id: 1)
        let request2 = MockRequest(id: 2)

        _ = sm.leaseConnection(request1)
        _ = sm.leaseConnection(request2)

        let cancelAction = sm.cancelRequest(2)

        if case .failRequest(let req, let error) = cancelAction.request {
            #expect(req.id == 2)
            #expect(error == .requestCancelled)
        } else {
            Issue.record("Should fail cancelled request")
        }
    }

    @Test("Connection above soft limit is closed on parking")
    func connectionAboveSoftLimitClosed() {
        var sm = makeStateMachine(softLimit: 1, hardLimit: 2)
        let conn1 = MockConnection(id: 0)
        let conn2 = MockConnection(id: 1)

        _ = sm.connectionEstablished(conn1)
        let action2 = sm.connectionEstablished(conn2)

        if case .closeConnection(let conn, _) = action2.connection {
            #expect(conn.id == 1)
        } else {
            Issue.record("Should close connection above soft limit when parked")
        }
    }

    @Test("Connection established services queued request")
    func connectionEstablishedServicesQueue() {
        var sm = makeStateMachine(hardLimit: 1)
        let request = MockRequest(id: 1)

        _ = sm.leaseConnection(request)

        let conn = MockConnection(id: 0)
        let action = sm.connectionEstablished(conn)

        if case .leaseConnection(let req, let leasedConn) = action.request {
            #expect(req.id == 1)
            #expect(leasedConn.id == 0)
        } else {
            Issue.record("Should lease to queued request")
        }
    }

    @Test("Cancel non-existent request returns none")
    func cancelNonExistentRequest() {
        var sm = makeStateMachine()
        let action = sm.cancelRequest(999)

        if case .none = action.request {
            // Expected
        } else {
            Issue.record("Should return none for non-existent request")
        }
    }

    @Test("Release unknown connection returns none")
    func releaseUnknownConnection() {
        var sm = makeStateMachine()
        let conn = MockConnection(id: 999)

        let action = sm.releaseConnection(conn)

        if case .none = action.request {
            // Expected
        } else {
            Issue.record("Should return none for unknown connection")
        }
    }

    @Test("Keep-alive failure closes connection")
    func keepAliveFailureClosesConnection() {
        var sm = makeStateMachine()
        let conn = MockConnection(id: 0)

        // Connection is parked (idle) after establishment with no queued requests
        _ = sm.connectionEstablished(conn)

        let action = sm.keepAliveFailed(0)

        if case .closeConnection(let closedConn, _) = action.connection {
            #expect(closedConn.id == 0)
        } else {
            Issue.record("Should close connection on keep-alive failure")
        }
    }

    @Test("Keep-alive success re-parks connection")
    func keepAliveSuccessReparksConnection() {
        var sm = makeStateMachine()
        let conn = MockConnection(id: 0)

        // Connection is parked (idle) after establishment with no queued requests
        _ = sm.connectionEstablished(conn)

        let action = sm.keepAliveSucceeded(conn)

        if case .scheduleTimers = action.connection {
            // Timers are rescheduled
        } else {
            Issue.record("Should reschedule timers on keep-alive success")
        }
    }

    @Test("Connection creation failure schedules backoff")
    func connectionCreationFailureSchedulesBackoff() {
        var sm = makeStateMachine()
        let request = MockRequest(id: 1)

        _ = sm.leaseConnection(request)

        struct TestError: Error {}
        let action = sm.connectionCreationFailed(0, error: TestError())

        if case .scheduleTimers = action.connection {
            // Expected - backoff timer scheduled
        } else {
            Issue.record("Should schedule backoff timer on failure")
        }
    }

    @Test("Circuit breaker trips after sustained failures")
    func circuitBreakerTrips() {
        let config = TestStateMachine.Configuration(
            minimumConnectionCount: 0,
            maximumConnectionSoftLimit: 4,
            maximumConnectionHardLimit: 4,
            keepAliveFrequency: nil,
            idleTimeout: .seconds(60),
            circuitBreakerTripAfter: .zero
        )
        var sm = TestStateMachine(configuration: config, idGenerator: ConnectionIDGenerator())

        let request = MockRequest(id: 1)
        _ = sm.leaseConnection(request)

        struct TestError: Error {}
        // First failure puts pool in connectionCreationFailing state
        _ = sm.connectionCreationFailed(0, error: TestError())
        // Second failure with zero trip duration opens circuit breaker
        let failAction = sm.connectionCreationFailed(0, error: TestError())

        // Circuit breaker should have tripped and failed the queued request
        if case .failRequests(let requests, let error) = failAction.request {
            #expect(requests.count == 1)
            #expect(error == .circuitBreakerTripped)
        } else {
            Issue.record("Should fail queued requests when circuit breaker trips")
        }

        // New requests should fail immediately
        let request2 = MockRequest(id: 2)
        let action = sm.leaseConnection(request2)

        if case .failRequest(_, let error) = action.request {
            #expect(error == .circuitBreakerTripped)
        } else {
            Issue.record("Should fail new requests when circuit breaker is open")
        }
    }

    @Test("Successful connection resets circuit breaker")
    func successfulConnectionResetsCircuitBreaker() {
        let config = TestStateMachine.Configuration(
            minimumConnectionCount: 0,
            maximumConnectionSoftLimit: 4,
            maximumConnectionHardLimit: 4,
            keepAliveFrequency: nil,
            idleTimeout: .seconds(60),
            circuitBreakerTripAfter: .zero
        )
        var sm = TestStateMachine(configuration: config, idGenerator: ConnectionIDGenerator())

        let request = MockRequest(id: 1)
        _ = sm.leaseConnection(request)

        struct TestError: Error {}
        _ = sm.connectionCreationFailed(0, error: TestError())

        let conn = MockConnection(id: 1)
        _ = sm.connectionEstablished(conn)
        _ = sm.releaseConnection(conn)

        let request2 = MockRequest(id: 2)
        let action = sm.leaseConnection(request2)

        if case .leaseConnection = action.request {
            // Expected - circuit breaker reset, connection leased
        } else {
            Issue.record("Should lease connection after circuit breaker reset")
        }
    }

    @Test("Idle timeout timer closes idle connection")
    func idleTimeoutClosesConnection() {
        var sm = makeStateMachine(idleTimeout: .seconds(60))
        let conn = MockConnection(id: 0)

        // connectionEstablished parks immediately when no requests queued
        let parkAction = sm.connectionEstablished(conn)

        guard case .scheduleTimers(let timers) = parkAction.connection else {
            Issue.record("Should schedule timers when parking")
            return
        }

        var idleTimer: TestStateMachine.Timer?
        for timer in timers {
            if timer.useCase == .idleTimeout {
                idleTimer = timer
                break
            }
        }

        guard let timer = idleTimer else {
            Issue.record("Should have idle timeout timer")
            return
        }

        let action = sm.timerTriggered(timer)

        if case .closeConnection(let closedConn, _) = action.connection {
            #expect(closedConn.id == 0)
        } else {
            Issue.record("Idle timeout should close connection")
        }
    }

    @Test("Backoff timer triggers connection retry")
    func backoffTimerTriggersRetry() {
        var sm = makeStateMachine()
        let request = MockRequest(id: 1)

        _ = sm.leaseConnection(request)

        struct TestError: Error {}
        let failAction = sm.connectionCreationFailed(0, error: TestError())

        // Extract the backoff timer
        guard case .scheduleTimers(let timers) = failAction.connection else {
            Issue.record("Should schedule backoff timer")
            return
        }

        var backoffTimer: TestStateMachine.Timer?
        for timer in timers {
            if timer.useCase == .backoff {
                backoffTimer = timer
                break
            }
        }

        guard let timer = backoffTimer else {
            Issue.record("Should have backoff timer")
            return
        }

        // Simulate timer firing
        let action = sm.timerTriggered(timer)

        if case .createConnection = action.connection {
            // Expected - retry connection creation
        } else {
            Issue.record("Backoff timer should trigger connection retry")
        }
    }

    @Test("Releasing already-closing connection does nothing")
    func releaseClosingConnection() {
        var sm = makeStateMachine()
        let conn = MockConnection(id: 0)

        _ = sm.connectionEstablished(conn)

        // Trigger shutdown to move connection to closing state
        _ = sm.triggerShutdown()

        // Try to release a connection that's being closed
        let action = sm.releaseConnection(conn)

        if case .none = action.request {
            // Expected - connection is closing, release does nothing
        } else {
            Issue.record("Should return none for closing connection")
        }
    }

    @Test("Keep-alive timer triggers keep-alive action")
    func keepAliveTimerTriggersKeepAlive() {
        let config = TestStateMachine.Configuration(
            minimumConnectionCount: 0,
            maximumConnectionSoftLimit: 4,
            maximumConnectionHardLimit: 4,
            keepAliveFrequency: .seconds(30),
            idleTimeout: .seconds(60),
            circuitBreakerTripAfter: .seconds(15)
        )
        var sm = TestStateMachine(configuration: config, idGenerator: ConnectionIDGenerator())

        let conn = MockConnection(id: 0)
        // connectionEstablished parks immediately when no requests queued
        let parkAction = sm.connectionEstablished(conn)

        guard case .scheduleTimers(let timers) = parkAction.connection else {
            Issue.record("Should schedule timers")
            return
        }

        var keepAliveTimer: TestStateMachine.Timer?
        for timer in timers {
            if timer.useCase == .keepAlive {
                keepAliveTimer = timer
                break
            }
        }

        guard let timer = keepAliveTimer else {
            Issue.record("Should have keep-alive timer")
            return
        }

        let action = sm.timerTriggered(timer)

        if case .runKeepAlive(let keepAliveConn) = action.connection {
            #expect(keepAliveConn.id == 0)
        } else {
            Issue.record("Keep-alive timer should trigger runKeepAlive")
        }
    }

    @Test("Timer triggered for unknown connection returns none")
    func timerForUnknownConnection() {
        var sm = makeStateMachine()

        let timer = TestStateMachine.Timer(
            timerID: 999,
            connectionID: 999,
            duration: .seconds(60),
            useCase: .idleTimeout
        )

        let action = sm.timerTriggered(timer)

        if case .none = action.connection {
            // Expected
        } else {
            Issue.record("Should return none for timer with unknown connection")
        }
    }

    @Test("Timer scheduled after connection leased does not crash")
    func timerScheduledAfterConnectionLeased() {
        var sm = makeStateMachine()
        let conn = MockConnection(id: 0)

        // Establish and park connection
        let parkAction = sm.connectionEstablished(conn)

        guard case .scheduleTimers(let timers) = parkAction.connection else {
            Issue.record("Should schedule timers")
            return
        }

        var idleTimer: TestStateMachine.Timer?
        for timer in timers {
            if timer.useCase == .idleTimeout {
                idleTimer = timer
                break
            }
        }

        guard let timer = idleTimer else {
            Issue.record("Should have idle timer")
            return
        }

        // Now lease the connection (makes it no longer idle)
        let request = MockRequest(id: 1)
        _ = sm.leaseConnection(request)

        // Calling timerScheduled after connection is leased should not crash
        let token = MockTimerToken(id: 1)
        sm.timerScheduled(timer, cancellationToken: token)

        // Should complete without error - reaching this point means no crash occurred
    }

    @Test("Double shutdown returns none")
    func doubleShutdown() {
        var sm = makeStateMachine()

        let action1 = sm.triggerShutdown()
        let action2 = sm.triggerShutdown()

        // First shutdown returns shutdown context
        if case .shutdown = action1.connection {
            // Expected
        } else {
            Issue.record("First shutdown should return shutdown action")
        }

        // Second shutdown returns none
        if case .none = action2.connection {
            // Expected
        } else {
            Issue.record("Second shutdown should return none")
        }
    }

    @Test("Shutdown with circuit breaker open fails queued requests")
    func shutdownWithCircuitBreakerOpen() {
        let config = TestStateMachine.Configuration(
            minimumConnectionCount: 0,
            maximumConnectionSoftLimit: 4,
            maximumConnectionHardLimit: 4,
            keepAliveFrequency: nil,
            idleTimeout: .seconds(60),
            circuitBreakerTripAfter: .zero
        )
        var sm = TestStateMachine(configuration: config, idGenerator: ConnectionIDGenerator())

        // Create request and fail connection twice to trip circuit breaker
        let request = MockRequest(id: 1)
        _ = sm.leaseConnection(request)

        struct TestError: Error {}
        _ = sm.connectionCreationFailed(0, error: TestError())
        _ = sm.connectionCreationFailed(0, error: TestError())

        // Add another request (will fail immediately due to circuit breaker)
        let request2 = MockRequest(id: 2)
        let leaseAction = sm.leaseConnection(request2)

        if case .failRequest(_, .circuitBreakerTripped) = leaseAction.request {
            // Expected
        } else {
            Issue.record("Should fail with circuit breaker tripped")
        }

        // Shutdown should still work
        let shutdownAction = sm.triggerShutdown()

        if case .shutdown = shutdownAction.connection {
            // Expected - shutdown proceeds even with circuit breaker open
        } else {
            Issue.record("Shutdown should proceed with circuit breaker open")
        }
    }

    @Test("Keep-alive succeeded on non-idle connection returns none")
    func keepAliveSucceededNonIdle() {
        var sm = makeStateMachine()
        let conn = MockConnection(id: 0)

        _ = sm.connectionEstablished(conn)

        // Lease the connection (no longer idle)
        let request = MockRequest(id: 1)
        _ = sm.leaseConnection(request)

        // Keep-alive succeeded on leased connection should return none
        let action = sm.keepAliveSucceeded(conn)

        if case .none = action.connection {
            // Expected
        } else {
            Issue.record("Should return none for non-idle connection")
        }
    }

    @Test("Keep-alive failed on non-idle connection returns none")
    func keepAliveFailedNonIdle() {
        var sm = makeStateMachine()
        let conn = MockConnection(id: 0)

        _ = sm.connectionEstablished(conn)

        // Lease the connection (no longer idle)
        let request = MockRequest(id: 1)
        _ = sm.leaseConnection(request)

        // Keep-alive failed on leased connection should return none
        let action = sm.keepAliveFailed(0)

        if case .none = action.connection {
            // Expected
        } else {
            Issue.record("Should return none for non-idle connection")
        }
    }

    @Test("Connection established during shutdown parks with timers")
    func connectionEstablishedDuringShutdown() {
        var sm = makeStateMachine()

        let request = MockRequest(id: 1)
        _ = sm.leaseConnection(request)

        // Shutdown clears request queue
        _ = sm.triggerShutdown()

        // Late-arriving connection is parked since queue is empty
        let conn = MockConnection(id: 0)
        let action = sm.connectionEstablished(conn)

        if case .scheduleTimers = action.connection {
            // Connection parked with idle/keep-alive timers
        } else {
            Issue.record("Should schedule timers for late connection during shutdown")
        }
    }

    @Test("Connection creation failed during shutdown returns none")
    func connectionCreationFailedDuringShutdown() {
        var sm = makeStateMachine()

        let request = MockRequest(id: 1)
        _ = sm.leaseConnection(request)

        _ = sm.triggerShutdown()

        struct TestError: Error {}
        let action = sm.connectionCreationFailed(0, error: TestError())

        if case .none = action.connection {
            // No backoff during shutdown
        } else {
            Issue.record("Should return none for connection failure during shutdown")
        }
    }

    @Test("Connection closed below minimum with pending requests triggers refill")
    func connectionClosedTriggersRefill() {
        var sm = makeStateMachine(minimumConnections: 2, hardLimit: 2)

        // Establish one connection
        let conn = MockConnection(id: 0)
        _ = sm.connectionEstablished(conn)

        // Lease the connection
        let request1 = MockRequest(id: 1)
        _ = sm.leaseConnection(request1)

        // Queue another request (will be pending since connection is leased and at limit)
        let request2 = MockRequest(id: 2)
        _ = sm.leaseConnection(request2)

        // Close the connection - should trigger refill since below minimum and has queue
        let action = sm.connectionClosed(0)

        if case .createConnection = action.connection {
            // Pool attempts to refill
        } else {
            Issue.record("Should create connection when below minimum with pending requests")
        }
    }

    @Test("Last connection closed during shutdown completes shutdown")
    func lastConnectionClosedCompletesShutdown() {
        var sm = makeStateMachine()
        let conn = MockConnection(id: 0)

        _ = sm.connectionEstablished(conn)

        // Lease the connection
        let request = MockRequest(id: 1)
        _ = sm.leaseConnection(request)

        // Shutdown - connection moves to closing state
        _ = sm.triggerShutdown()

        // Close the connection - should complete shutdown
        _ = sm.connectionClosed(0)

        // New lease should fail with poolShutdown
        let request2 = MockRequest(id: 2)
        let action = sm.leaseConnection(request2)

        if case .failRequest(_, .poolShutdown) = action.request {
            // Pool is fully shut down
        } else {
            Issue.record("Should fail request after shutdown completes")
        }
    }
}

@Suite("Backoff Calculation Tests")
struct BackoffCalculationTests {

    @Test("First attempt backoff is around 100ms")
    func firstAttemptBackoff() {
        let backoff = TestStateMachine.calculateBackoff(failedAttempt: 1)
        let nanoseconds = backoff.components.attoseconds / 1_000_000_000

        #expect(nanoseconds >= 90_000_000)
        #expect(nanoseconds <= 110_000_000)
    }

    @Test("Backoff increases with attempts")
    func backoffIncreases() {
        let backoff1 = TestStateMachine.calculateBackoff(failedAttempt: 1)
        let backoff5 = TestStateMachine.calculateBackoff(failedAttempt: 5)
        let backoff10 = TestStateMachine.calculateBackoff(failedAttempt: 10)

        #expect(backoff5 > backoff1)
        #expect(backoff10 > backoff5)
    }

    @Test("Backoff is capped at 60 seconds")
    func backoffIsCapped() {
        let backoff = TestStateMachine.calculateBackoff(failedAttempt: 100)
        #expect(backoff <= .seconds(62))
    }
}

@Suite("Connection Pool Error Tests")
struct ConnectionPoolErrorTests {

    @Test("Errors are equatable")
    func errorsAreEquatable() {
        #expect(ConnectionPoolError.requestCancelled == ConnectionPoolError.requestCancelled)
        #expect(ConnectionPoolError.poolShutdown == ConnectionPoolError.poolShutdown)
        #expect(ConnectionPoolError.requestCancelled != ConnectionPoolError.poolShutdown)
    }

    @Test("Errors are hashable")
    func errorsAreHashable() {
        let set: Set<ConnectionPoolError> = [.requestCancelled, .poolShutdown, .circuitBreakerTripped]
        #expect(set.count == 3)
    }
}

@Suite("Configuration Tests")
struct ConfigurationTests {

    @Test("Default configuration has reasonable values")
    func defaultConfiguration() {
        let config = ConnectionPoolConfiguration.default

        #expect(config.minimumConnectionCount == 0)
        #expect(config.maximumConnectionSoftLimit == 4)
        #expect(config.maximumConnectionHardLimit == 4)
        #expect(config.idleTimeout == .seconds(60))
    }

    @Test("Configuration is customizable")
    func customConfiguration() {
        let config = ConnectionPoolConfiguration(
            minimumConnectionCount: 2,
            maximumConnectionSoftLimit: 10,
            maximumConnectionHardLimit: 20,
            idleTimeout: .seconds(120)
        )

        #expect(config.minimumConnectionCount == 2)
        #expect(config.maximumConnectionSoftLimit == 10)
        #expect(config.maximumConnectionHardLimit == 20)
        #expect(config.idleTimeout == .seconds(120))
    }
}

@Suite("Utility Tests")
struct UtilityTests {

    @Test("TinyFastSequence handles empty case")
    func tinyFastSequenceEmpty() {
        let seq = TinyFastSequence<Int>()
        #expect(seq.isEmpty)
        #expect(seq.count == 0)
        #expect(Array(seq) == [])
    }

    @Test("TinyFastSequence handles single element")
    func tinyFastSequenceSingle() {
        let seq = TinyFastSequence(42)
        #expect(!seq.isEmpty)
        #expect(seq.count == 1)
        #expect(Array(seq) == [42])
    }

    @Test("TinyFastSequence handles two elements")
    func tinyFastSequenceTwo() {
        let seq = TinyFastSequence(contentsOf: [1, 2])
        #expect(seq.count == 2)
        #expect(Array(seq) == [1, 2])
    }

    @Test("TinyFastSequence handles multiple elements")
    func tinyFastSequenceMultiple() {
        let seq = TinyFastSequence(contentsOf: [1, 2, 3, 4, 5])
        #expect(seq.count == 5)
        #expect(Array(seq) == [1, 2, 3, 4, 5])
    }

    @Test("TinyFastSequence append works")
    func tinyFastSequenceAppend() {
        var seq = TinyFastSequence<Int>()
        seq.append(1)
        seq.append(2)
        seq.append(3)
        #expect(Array(seq) == [1, 2, 3])
    }

    @Test("Max2Sequence handles zero to two elements")
    func max2Sequence() {
        var seq = Max2Sequence<Int>()
        #expect(seq.isEmpty)

        seq.append(1)
        #expect(Array(seq) == [1])

        seq.append(2)
        #expect(Array(seq) == [1, 2])
    }

    @Test("ConnectionIDGenerator produces unique IDs")
    func idGeneratorUnique() {
        let gen = ConnectionIDGenerator()
        let id1 = gen.next()
        let id2 = gen.next()
        let id3 = gen.next()

        #expect(id1 != id2)
        #expect(id2 != id3)
        #expect(id1 != id3)
    }

    @Test("ConnectionIDGenerator is monotonic")
    func idGeneratorMonotonic() {
        let gen = ConnectionIDGenerator()
        let id1 = gen.next()
        let id2 = gen.next()
        let id3 = gen.next()

        #expect(id2 == id1 + 1)
        #expect(id3 == id2 + 1)
    }
}

@Suite("Request Queue Tests")
struct RequestQueueTests {

    @Test("Empty queue has count zero")
    func emptyQueue() {
        let queue = RequestQueue<MockRequest>()
        #expect(queue.isEmpty)
        #expect(queue.count == 0)
    }

    @Test("Enqueue increases count")
    func enqueueIncreasesCount() {
        var queue = RequestQueue<MockRequest>()
        queue.enqueue(MockRequest(id: 1))
        queue.enqueue(MockRequest(id: 2))
        #expect(queue.count == 2)
    }

    @Test("PopFirst returns requests in FIFO order")
    func popFirstFIFO() {
        var queue = RequestQueue<MockRequest>()
        queue.enqueue(MockRequest(id: 1))
        queue.enqueue(MockRequest(id: 2))
        queue.enqueue(MockRequest(id: 3))

        #expect(queue.popFirst()?.id == 1)
        #expect(queue.popFirst()?.id == 2)
        #expect(queue.popFirst()?.id == 3)
        #expect(queue.popFirst() == nil)
    }

    @Test("Cancel removes request from queue")
    func cancelRemovesRequest() {
        var queue = RequestQueue<MockRequest>()
        queue.enqueue(MockRequest(id: 1))
        queue.enqueue(MockRequest(id: 2))
        queue.enqueue(MockRequest(id: 3))

        let cancelled = queue.cancel(2)
        #expect(cancelled?.id == 2)
        #expect(queue.count == 2)
    }

    @Test("Cancel non-existent request returns nil")
    func cancelNonExistent() {
        var queue = RequestQueue<MockRequest>()
        queue.enqueue(MockRequest(id: 1))

        let cancelled = queue.cancel(999)
        #expect(cancelled == nil)
        #expect(queue.count == 1)
    }

    @Test("PopFirst skips cancelled requests (tombstones)")
    func popFirstSkipsTombstones() {
        var queue = RequestQueue<MockRequest>()
        queue.enqueue(MockRequest(id: 1))
        queue.enqueue(MockRequest(id: 2))
        queue.enqueue(MockRequest(id: 3))

        // Cancel the middle request
        _ = queue.cancel(2)

        // Should skip the cancelled request
        #expect(queue.popFirst()?.id == 1)
        #expect(queue.popFirst()?.id == 3)
        #expect(queue.popFirst() == nil)
    }

    @Test("Cancel after popFirst works correctly")
    func cancelAfterPop() {
        var queue = RequestQueue<MockRequest>()
        queue.enqueue(MockRequest(id: 1))
        queue.enqueue(MockRequest(id: 2))
        queue.enqueue(MockRequest(id: 3))

        _ = queue.popFirst()  // Remove 1
        _ = queue.cancel(3)   // Cancel 3

        #expect(queue.count == 1)
        #expect(queue.popFirst()?.id == 2)
        #expect(queue.popFirst() == nil)
    }

    @Test("RemoveAll returns all pending requests")
    func removeAllReturnsAll() {
        var queue = RequestQueue<MockRequest>()
        queue.enqueue(MockRequest(id: 1))
        queue.enqueue(MockRequest(id: 2))
        queue.enqueue(MockRequest(id: 3))

        let all = queue.removeAll()
        #expect(all.count == 3)
        #expect(queue.isEmpty)
    }

    @Test("RemoveAll excludes cancelled requests")
    func removeAllExcludesCancelled() {
        var queue = RequestQueue<MockRequest>()
        queue.enqueue(MockRequest(id: 1))
        queue.enqueue(MockRequest(id: 2))
        queue.enqueue(MockRequest(id: 3))

        _ = queue.cancel(2)

        let all = queue.removeAll()
        #expect(all.count == 2)
        #expect(all.contains { $0.id == 1 })
        #expect(all.contains { $0.id == 3 })
        #expect(!all.contains { $0.id == 2 })
    }

    @Test("Multiple cancellations work correctly")
    func multipleCancellations() {
        var queue = RequestQueue<MockRequest>()
        for i in 1...10 {
            queue.enqueue(MockRequest(id: i))
        }

        // Cancel all even numbers
        for i in stride(from: 2, through: 10, by: 2) {
            _ = queue.cancel(i)
        }

        #expect(queue.count == 5)

        // Should only get odd numbers
        var results: [Int] = []
        while let req = queue.popFirst() {
            results.append(req.id)
        }

        #expect(results == [1, 3, 5, 7, 9])
    }

    @Test("Double cancel returns nil on second attempt")
    func doubleCancelReturnsNil() {
        var queue = RequestQueue<MockRequest>()
        queue.enqueue(MockRequest(id: 1))

        let first = queue.cancel(1)
        let second = queue.cancel(1)

        #expect(first?.id == 1)
        #expect(second == nil)
    }
}

@Suite("Connection Lease Tests")
struct ConnectionLeaseTests {

    @Test("Lease holds connection reference")
    func leaseHoldsConnection() {
        let conn = MockConnection(id: 42)
        let released = LockedValueBox(false)

        let lease = ConnectionLease(connection: conn) { _ in
            released.withLockedValue { $0 = true }
        }

        #expect(lease.connection.id == 42)
        #expect(!released.withLockedValue { $0 })
    }

    @Test("Lease release calls handler")
    func leaseReleaseCallsHandler() {
        let conn = MockConnection(id: 42)
        let released = LockedValueBox(false)

        let lease = ConnectionLease(connection: conn) { _ in
            released.withLockedValue { $0 = true }
        }

        lease.release()
        #expect(released.withLockedValue { $0 })
    }
}
