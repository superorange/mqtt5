import 'dart:async';

import '../exception/mqtt_exception.dart';

/// Enforces the server's Receive Maximum for outgoing QoS 1/2 publications.
///
/// A slot is acquired before a PUBLISH is sent and released when its
/// acknowledgement completes (PUBACK/PUBCOMP, or a PUBREC error).
final class FlowController {
  int receiveMaximum;

  FlowController({this.receiveMaximum = 65535});

  int _outstanding = 0;
  Completer<void>? _waiter;

  int get outstanding => _outstanding;

  /// Acquires a send slot, waiting until one is available.
  ///
  /// When [deadline] passes first, [MqttTimeoutException] is thrown and no
  /// slot is taken, so a caller that gives up cannot leak quota.
  Future<void> acquire({DateTime? deadline}) async {
    while (_outstanding >= receiveMaximum) {
      if (deadline != null) {
        final remaining = deadline.difference(DateTime.now());
        if (remaining <= Duration.zero) {
          throw MqttTimeoutException(
            'Timed out waiting for a Receive Maximum slot',
          );
        }
        final waiter = _waiter ??= Completer<void>();
        await waiter.future.timeout(remaining, onTimeout: () {});
      } else {
        final waiter = _waiter ??= Completer<void>();
        await waiter.future;
      }
    }
    _outstanding++;
  }

  /// Releases a slot, waking one waiter if any.
  void release() {
    if (_outstanding > 0) {
      _outstanding--;
    }
    final waiter = _waiter;
    if (waiter != null && !waiter.isCompleted) {
      _waiter = null;
      waiter.complete();
    }
  }

  /// Resets the outstanding count (called when a connection is lost) and
  /// wakes any blocked acquirers.
  void reset() {
    _outstanding = 0;
    final waiter = _waiter;
    if (waiter != null && !waiter.isCompleted) {
      _waiter = null;
      waiter.complete();
    }
  }
}
