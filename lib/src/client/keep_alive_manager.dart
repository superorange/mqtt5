import 'dart:async';

/// Drives MQTT keep alive: sends PINGREQ when the connection is idle and
/// reports a timeout when no PINGRESP arrives.
final class KeepAliveManager {
  KeepAliveManager({
    required this.onPingRequired,
    required this.onPingTimeout,
  });

  /// Called when a PINGREQ should be sent.
  final void Function() onPingRequired;

  /// Called when a PINGRESP has not arrived within the keep alive interval.
  final void Function() onPingTimeout;

  Timer? _timer;
  Duration _keepAlive = Duration.zero;
  bool _pingOutstanding = false;
  bool _running = false;

  bool get isRunning => _running;

  Duration get keepAlive => _keepAlive;

  /// Starts keep alive with [keepAlive]. A zero duration disables it.
  void start(Duration keepAlive) {
    _keepAlive = keepAlive;
    _pingOutstanding = false;
    if (keepAlive <= Duration.zero) {
      _running = false;
      _timer?.cancel();
      _timer = null;
      return;
    }
    _running = true;
    _schedule();
  }

  /// Overrides the keep alive interval (server keep alive from CONNACK).
  void updateKeepAlive(Duration keepAlive) {
    if (keepAlive == _keepAlive) {
      return;
    }
    start(keepAlive);
  }

  void stop() {
    _running = false;
    _pingOutstanding = false;
    _timer?.cancel();
    _timer = null;
  }

  /// Notifies that an outbound control packet was sent.
  void onOutboundActivity() {
    if (!_running) {
      return;
    }
    if (!_pingOutstanding) {
      _schedule();
    }
  }

  /// Notifies that a PINGRESP was received.
  void onPingResponse() {
    if (!_running) {
      return;
    }
    _pingOutstanding = false;
    _schedule();
  }

  void _schedule() {
    _timer?.cancel();
    _timer = Timer(_keepAlive, _onTick);
  }

  void _onTick() {
    if (!_running) {
      return;
    }
    if (_pingOutstanding) {
      _pingOutstanding = false;
      onPingTimeout();
      return;
    }
    _pingOutstanding = true;
    onPingRequired();
    _schedule();
  }
}
