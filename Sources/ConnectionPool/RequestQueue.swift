// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin
//
// Portions derived from PostgresNIO (licensed under the MIT License)
// Copyright (c) 2017-2024 Vapor

import DequeModule

/// A request queue that provides O(1) enqueue, dequeue, and cancellation.
///
/// Uses a deque for ordering and a dictionary for fast lookup. Cancelled requests
/// are removed from the dictionary but left in the deque as "tombstones", which
/// are skipped during dequeue operations.
struct RequestQueue<Request: ConnectionRequestProtocol>: Sendable {
    private var queue: Deque<Request.ID>
    private var requests: [Request.ID: Request]

    init() {
        self.queue = Deque(minimumCapacity: 32)
        self.requests = Dictionary(minimumCapacity: 32)
    }

    /// Number of pending (non-cancelled) requests
    var count: Int {
        requests.count
    }

    /// Whether there are any pending requests
    var isEmpty: Bool {
        requests.isEmpty
    }

    /// Add a request to the end of the queue. O(1).
    mutating func enqueue(_ request: Request) {
        queue.append(request.id)
        requests[request.id] = request
    }

    /// Remove and return the next pending request, skipping cancelled ones. O(1) amortized.
    mutating func popFirst() -> Request? {
        while let requestID = queue.popFirst() {
            if let request = requests.removeValue(forKey: requestID) {
                return request
            }
            // Request was cancelled (tombstone), skip it
        }
        return nil
    }

    /// Cancel a request by ID. Returns the request if found. O(1).
    mutating func cancel(_ requestID: Request.ID) -> Request? {
        // Remove from dictionary only; ID remains in deque as tombstone
        return requests.removeValue(forKey: requestID)
    }

    /// Remove all requests and return them.
    mutating func removeAll() -> [Request] {
        let result = Array(requests.values)
        queue.removeAll()
        requests.removeAll()
        return result
    }
}
