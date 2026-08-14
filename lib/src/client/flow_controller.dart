import 'dart:async';

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
  Future<void> acquire() async {
    while (_outstanding >= receiveMaximum) {
      final waiter = _waiter ??= Completer<void>();
      await waiter.future;
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
