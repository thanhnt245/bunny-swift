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

// MARK: - TinyFastSequence

/// A sequence optimized for 0-2 elements, avoiding heap allocation for small sizes.
@usableFromInline
struct TinyFastSequence<Element: Sendable>: Sequence, Sendable {
  @usableFromInline
  enum Storage: Sendable {
    case none
    case one(Element)
    case two(Element, Element)
    case array([Element])
  }

  @usableFromInline
  var storage: Storage

  @inlinable
  init() {
    self.storage = .none
  }

  @inlinable
  init(_ element: Element) {
    self.storage = .one(element)
  }

  @inlinable
  init<S: Sequence>(contentsOf elements: S) where S.Element == Element {
    let arr = Array(elements)
    switch arr.count {
    case 0: self.storage = .none
    case 1: self.storage = .one(arr[0])
    case 2: self.storage = .two(arr[0], arr[1])
    default: self.storage = .array(arr)
    }
  }

  @inlinable
  init(_ elements: [Element]) {
    switch elements.count {
    case 0: self.storage = .none
    case 1: self.storage = .one(elements[0])
    case 2: self.storage = .two(elements[0], elements[1])
    default: self.storage = .array(elements)
    }
  }

  @inlinable
  var isEmpty: Bool {
    if case .none = storage { return true }
    return false
  }

  @inlinable
  var count: Int {
    switch storage {
    case .none: return 0
    case .one: return 1
    case .two: return 2
    case .array(let arr): return arr.count
    }
  }

  @inlinable
  mutating func append(_ element: Element) {
    switch storage {
    case .none:
      storage = .one(element)
    case .one(let first):
      storage = .two(first, element)
    case .two(let first, let second):
      storage = .array([first, second, element])
    case .array(var arr):
      arr.append(element)
      storage = .array(arr)
    }
  }

  @inlinable
  mutating func reserveCapacity(_ minimumCapacity: Int) {
    if minimumCapacity > 2 {
      switch storage {
      case .none:
        var arr: [Element] = []
        arr.reserveCapacity(minimumCapacity)
        storage = .array(arr)
      case .one(let e):
        var arr = [e]
        arr.reserveCapacity(minimumCapacity)
        storage = .array(arr)
      case .two(let e1, let e2):
        var arr = [e1, e2]
        arr.reserveCapacity(minimumCapacity)
        storage = .array(arr)
      case .array(var arr):
        arr.reserveCapacity(minimumCapacity)
        storage = .array(arr)
      }
    }
  }

  @inlinable
  func makeIterator() -> AnyIterator<Element> {
    switch storage {
    case .none:
      return AnyIterator { nil }
    case .one(let e):
      var done = false
      return AnyIterator {
        if done { return nil }
        done = true
        return e
      }
    case .two(let e1, let e2):
      var index = 0
      return AnyIterator {
        defer { index += 1 }
        switch index {
        case 0: return e1
        case 1: return e2
        default: return nil
        }
      }
    case .array(let arr):
      var index = 0
      return AnyIterator {
        guard index < arr.count else { return nil }
        defer { index += 1 }
        return arr[index]
      }
    }
  }
}

// MARK: - Max2Sequence

/// A sequence that holds at most 2 elements.
@usableFromInline
struct Max2Sequence<Element: Sendable>: Sequence, Sendable {
  @usableFromInline
  enum Storage: Sendable {
    case none
    case one(Element)
    case two(Element, Element)
  }

  @usableFromInline
  var storage: Storage

  @inlinable
  init() {
    self.storage = .none
  }

  @inlinable
  init(_ element: Element) {
    self.storage = .one(element)
  }

  @inlinable
  init(_ first: Element, _ second: Element) {
    self.storage = .two(first, second)
  }

  @inlinable
  var isEmpty: Bool {
    if case .none = storage { return true }
    return false
  }

  @inlinable
  mutating func append(_ element: Element) {
    switch storage {
    case .none:
      storage = .one(element)
    case .one(let first):
      storage = .two(first, element)
    case .two:
      fatalError("Max2Sequence cannot hold more than 2 elements")
    }
  }

  @inlinable
  func makeIterator() -> AnyIterator<Element> {
    switch storage {
    case .none:
      return AnyIterator { nil }
    case .one(let e):
      var done = false
      return AnyIterator {
        if done { return nil }
        done = true
        return e
      }
    case .two(let e1, let e2):
      var index = 0
      return AnyIterator {
        defer { index += 1 }
        switch index {
        case 0: return e1
        case 1: return e2
        default: return nil
        }
      }
    }
  }
}

// MARK: - LockedValueBox

/// A thread-safe container for mutable state.
public final class LockedValueBox<Value: Sendable>: @unchecked Sendable {
  @usableFromInline
  var value: Value

  @usableFromInline
  let lock = NSLock()

  @inlinable
  public init(_ value: Value) {
    self.value = value
  }

  @inlinable
  @discardableResult
  public func withLockedValue<T>(_ body: (inout Value) throws -> T) rethrows -> T {
    lock.lock()
    defer { lock.unlock() }
    return try body(&value)
  }
}
