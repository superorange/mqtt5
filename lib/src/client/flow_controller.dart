import 'dart:async';
import 'dart:collection';

import '../exception/mqtt_exception.dart';

/// The send quota derived from the server's Receive Maximum (section 4.9).
///
/// A slot is taken before a QoS 1/2 PUBLISH is sent and given back when its
/// acknowledgement completes (PUBACK/PUBCOMP, or a PUBREC error). The quota
/// restricts PUBLISH packets only: MQTT-3.3.4-8 forbids delaying any other
/// packet because the quota is exhausted, so PUBREL is sent without consulting
/// it.
///
/// The quota is per network connection, not per session (section 4.9), which
/// is why [reset] runs on every connection loss and every resumed session.
final class FlowController {
  int receiveMaximum;

  FlowController({this.receiveMaximum = 65535});

  int _outstanding = 0;

  /// Waiters in arrival order.
  ///
  /// One completer per waiter rather than one shared completer for all of
  /// them: releasing a single slot should wake the one caller that can use it,
  /// not every caller so that all but one immediately go back to waiting. With
  /// a shared completer, N blocked publishes cost O(N) wakeups per release and
  /// O(N^2) over the queue.
  final ListQueue<Completer<void>> _waiters = ListQueue<Completer<void>>();

  int get outstanding => _outstanding;

  /// The number of callers currently blocked in [acquire].
  int get waiting => _waiters.length;

  /// Acquires a send slot, waiting until one is available.
  ///
  /// When [deadline] passes first, [MqttTimeoutException] is thrown and no
  /// slot is taken, so a caller that gives up cannot leak quota.
  Future<void> acquire({DateTime? deadline}) async {
    while (_outstanding >= receiveMaximum) {
      final waiter = Completer<void>();
      _waiters.addLast(waiter);
      if (deadline != null) {
        final remaining = deadline.difference(DateTime.now());
        if (remaining <= Duration.zero) {
          _waiters.remove(waiter);
          throw MqttTimeoutException(
            'Timed out waiting for a Receive Maximum slot',
          );
        }
        await waiter.future.timeout(remaining, onTimeout: () {});
        // Whether it was woken or timed out, this waiter is done with the
        // queue. Dropping it here keeps [release] from handing a slot to a
        // caller that has already given up, which would strand that slot.
        _waiters.remove(waiter);
      } else {
        await waiter.future;
      }
    }
    _outstanding++;
  }

  /// Takes a slot if one is free, without waiting. Returns false when the
  /// quota is exhausted.
  bool tryAcquire() {
    if (_outstanding >= receiveMaximum) {
      return false;
    }
    _outstanding++;
    return true;
  }

  /// Releases a slot, waking one waiter if any.
  ///
  /// The quota is never incremented above its initial value, matching the
  /// clamp the specification describes in section 4.9 for the PUBCOMP that
  /// answers a PUBREL retransmitted on a new network connection.
  void release() {
    if (_outstanding > 0) {
      _outstanding--;
    }
    _wakeOne();
  }

  /// Resets the outstanding count (called when a connection is lost) and
  /// wakes any blocked acquirers.
  void reset() {
    _outstanding = 0;
    // Every waiter can make progress now, so wake all of them rather than
    // handing the whole freed quota to the first in line.
    while (_waiters.isNotEmpty) {
      final waiter = _waiters.removeFirst();
      if (!waiter.isCompleted) {
        waiter.complete();
      }
    }
  }

  /// Wakes the longest-waiting caller that is still waiting.
  void _wakeOne() {
    while (_waiters.isNotEmpty) {
      final waiter = _waiters.removeFirst();
      if (!waiter.isCompleted) {
        waiter.complete();
        return;
      }
    }
  }
}
