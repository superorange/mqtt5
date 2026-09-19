import 'dart:async';

/// Drives MQTT keep alive: sends PINGREQ when the connection is idle and
/// reports a timeout when no PINGRESP arrives.
final class KeepAliveManager {
  KeepAliveManager({
    required this.onPingRequired,
    required this.onPingTimeout,
    this.pingResponseTimeout,
  }) {
    final timeout = pingResponseTimeout;
    if (timeout != null && timeout <= Duration.zero) {
      throw ArgumentError.value(
        timeout,
        'pingResponseTimeout',
        'Must be greater than zero',
      );
    }
  }

  /// Called when a PINGREQ should be sent.
  final void Function() onPingRequired;

  /// Called when a PINGRESP has not arrived in time.
  final void Function() onPingTimeout;

  /// How long to wait for a PINGRESP before declaring the link dead.
  ///
  /// Null keeps the keep alive interval itself as the deadline, which means a
  /// silently dropped link takes two intervals to notice: one for the idle
  /// timer to fire the PINGREQ, another for the answer that never comes. That
  /// is fine at 60 seconds and poor at 300, so a client that wants death
  /// detected on its own schedule sets this instead of shortening keep alive
  /// (which would also raise the traffic floor).
  final Duration? pingResponseTimeout;

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

  /// The wait that applies once a PINGREQ is outstanding.
  Duration get effectivePingResponseTimeout => pingResponseTimeout ?? _keepAlive;

  void _schedule([Duration? delay]) {
    _timer?.cancel();
    _timer = Timer(delay ?? _keepAlive, _onTick);
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
    // The next tick is the PINGRESP deadline, not another idle period.
    _schedule(effectivePingResponseTimeout);
  }
}
