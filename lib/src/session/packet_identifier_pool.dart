import 'dart:async';

import '../exception/mqtt_exception.dart';

/// Allocates MQTT packet identifiers (1..65535).
///
/// An identifier remains reserved until [release] is called, so it is never
/// re-used while still in flight.
final class PacketIdentifierPool {
  static const int maxIdentifier = 0xFFFF;

  final Set<int> _inUse = <int>{};
  int _next = 1;
  Completer<void>? _releaseSignal;

  int get inUseCount => _inUse.length;

  /// Attempts to allocate an identifier, or returns null if exhausted.
  int? tryAllocate() {
    for (var i = 0; i < maxIdentifier; i++) {
      final candidate = _next;
      _next = _next == maxIdentifier ? 1 : _next + 1;
      if (_inUse.add(candidate)) {
        return candidate;
      }
    }
    return null;
  }

  /// Allocates an identifier, waiting until one becomes available.
  Future<int> allocate() async {
    while (true) {
      final id = tryAllocate();
      if (id != null) {
        return id;
      }
      final signal = _releaseSignal ??= Completer<void>();
      await signal.future;
    }
  }

  /// Reserves [identifier] explicitly, rejecting identifiers already in use
  /// or out of range.
  void reserve(int identifier) {
    if (identifier < 1 || identifier > maxIdentifier) {
      throw MqttFlowControlException(
        'Packet identifier out of range: $identifier',
      );
    }
    if (!_inUse.add(identifier)) {
      throw MqttFlowControlException(
        'Packet identifier already in use: $identifier',
      );
    }
  }

  bool isInUse(int identifier) => _inUse.contains(identifier);

  /// Releases [identifier], waking any waiter.
  void release(int identifier) {
    if (_inUse.remove(identifier)) {
      final signal = _releaseSignal;
      if (signal != null && !signal.isCompleted) {
        _releaseSignal = null;
        signal.complete();
      }
    }
  }

  /// Releases every identifier (e.g. on session reset).
  void reset() {
    _inUse.clear();
    _next = 1;
  }
}
