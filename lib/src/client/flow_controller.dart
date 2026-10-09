import 'dart:async';
import 'dart:collection';

import '../exception/mqtt_exception.dart';

/// The send quota derived from the server's Receive Maximum (section 4.9).
///
/// A slot is taken before a QoS 1/2 PUBLISH is sent and given back when its
/// acknowledgement completes (PUBACK/PUBCOMP, or a PUBREC error). The quota
/// restricts PUBLISH packets only: MQTT-3.3.4-8 forbids delaying any other
/// packet because the quota is exhausted, so PUBREL never consults it.
///
/// Slots are handed out strictly first come, first served. A caller that
/// finds a free slot while others are queued still queues behind them, and a
/// released slot is not given to a waiter directly: the owner first lets a
/// resumed session's backlog (older publications) use it, then calls
/// [dispatch]. Both rules keep publications on the wire in the order the
/// application made them.
///
/// The quota is per network connection (section 4.9). While there is no
/// connection the controller is [suspend]ed: waiters keep their place instead
/// of failing, and are served when the next connection [resume]s.
final class FlowController {
  FlowController({this.receiveMaximum = 65535});

  int receiveMaximum;
  int _outstanding = 0;
  bool _open = false;
  final ListQueue<_Waiter> _waiters = ListQueue<_Waiter>();

  /// Takes a slot, waiting behind earlier callers while the quota is used up
  /// or there is no connection.
  ///
  /// When [deadline] passes first, [MqttTimeoutException] is thrown and no
  /// slot is taken.
  Future<void> acquire({DateTime? deadline}) {
    if (_open && _waiters.isEmpty && _outstanding < receiveMaximum) {
      _outstanding++;
      return Future<void>.value();
    }
    final waiter = _Waiter();
    _waiters.addLast(waiter);
    if (deadline != null) {
      final remaining = deadline.difference(DateTime.now());
      waiter.timer = Timer(
        remaining.isNegative ? Duration.zero : remaining,
        () {
          if (_waiters.remove(waiter)) {
            waiter.completer.completeError(
              MqttTimeoutException(
                  'Timed out waiting for a Receive Maximum slot'),
            );
          }
        },
      );
    }
    return waiter.completer.future;
  }

  /// Takes a slot if one is free, ignoring queued callers. Reserved for
  /// retransmissions, which are older than anything waiting.
  bool tryAcquire() {
    if (!_open || _outstanding >= receiveMaximum) {
      return false;
    }
    _outstanding++;
    return true;
  }

  /// Gives a slot back. Call [dispatch] afterwards to serve waiters.
  ///
  /// Never goes below zero: the PUBCOMP answering a PUBREL re-sent on a new
  /// connection releases a slot that connection never took (section 4.9).
  void release() {
    if (_outstanding > 0) {
      _outstanding--;
    }
  }

  /// Hands free slots to waiters in arrival order.
  void dispatch() {
    while (_open && _waiters.isNotEmpty && _outstanding < receiveMaximum) {
      final waiter = _waiters.removeFirst();
      waiter.timer?.cancel();
      _outstanding++;
      waiter.completer.complete();
    }
  }

  /// The connection is gone: no slot is outstanding any more, and waiters
  /// hold their place until [resume].
  void suspend() {
    _open = false;
    _outstanding = 0;
  }

  /// A new connection with the server's [receiveMaximum] for it. Waiters are
  /// not served until [dispatch], so a resumed backlog can go first.
  void resume(int receiveMaximum) {
    this.receiveMaximum = receiveMaximum;
    _outstanding = 0;
    _open = true;
  }

  /// Fails every waiter with [error] (explicit disconnect, close, fatal end).
  void failWaiters(Object error) {
    while (_waiters.isNotEmpty) {
      final waiter = _waiters.removeFirst();
      waiter.timer?.cancel();
      waiter.completer.completeError(error);
    }
  }
}

final class _Waiter {
  final Completer<void> completer = Completer<void>();
  Timer? timer;
}
