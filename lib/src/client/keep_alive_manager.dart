import 'dart:async';

/// Drives MQTT keep alive: sends PINGREQ when the link has been quiet and
/// reports a timeout when the link stays silent after one.
///
/// Three rules, all measured on a monotonic clock so wall-clock jumps cannot
/// trigger or suppress a ping:
///
/// * MQTT-3.1.2-20: whenever [keepAlive] passes without the client sending
///   anything, a PINGREQ is sent — even while an earlier PINGREQ is still
///   unanswered. Otherwise a PINGRESP stuck behind a large transfer on a slow
///   link keeps the client silent until the broker drops it (1.5 x keep
///   alive).
/// * A PINGREQ is also due once [keepAlive] passes without receiving
///   anything. This is what lets a client that only publishes notice a dead
///   path: outbound writes succeed into the socket buffer long after the peer
///   is gone.
/// * Once a PINGREQ is outstanding, the link is declared dead only after
///   [pingResponseTimeout] (or [keepAlive]) with no inbound bytes at all. Any
///   inbound traffic proves the path is alive and pushes the deadline back;
///   a PINGRESP queued behind a large transfer is not a dead link.
final class KeepAliveManager {
  KeepAliveManager({
    required this.onPingRequired,
    required this.onPingTimeout,
    this.pingResponseTimeout,
  });

  /// Called when a PINGREQ should be sent.
  final void Function() onPingRequired;

  /// Called when no inbound traffic arrived in time after a PINGREQ.
  final void Function() onPingTimeout;

  /// How long the link may stay silent after a PINGREQ before it is declared
  /// dead. Null uses the keep alive interval.
  final Duration? pingResponseTimeout;

  final Stopwatch _clock = Stopwatch()..start();
  Timer? _timer;
  Duration _keepAlive = Duration.zero;
  Duration _lastOutbound = Duration.zero;
  Duration _lastInbound = Duration.zero;
  Duration? _pingDeadline;
  bool _running = false;

  /// The wait that applies once a PINGREQ is outstanding.
  Duration get effectivePingResponseTimeout =>
      pingResponseTimeout ?? _keepAlive;

  /// Monotonic time since this manager was created.
  ///
  /// The same clock [start] and the activity callbacks use. Capture it when
  /// CONNECT is written and pass it back as [start]'s `outboundAt`, so the
  /// first PINGREQ is measured from that write rather than from CONNACK.
  Duration get monotonicNow => _clock.elapsed;

  /// Starts keep alive with [keepAlive]. A zero duration disables it.
  ///
  /// [outboundAt] is the monotonic time of the last outbound packet, normally
  /// the CONNECT write. A value in the future is clamped to now. A value
  /// already [keepAlive] in the past arms the first PINGREQ immediately.
  /// Omit it to start the interval from now.
  void start(Duration keepAlive, {Duration? outboundAt}) {
    _keepAlive = keepAlive;
    _pingDeadline = null;
    _timer?.cancel();
    _timer = null;
    _running = keepAlive > Duration.zero;
    final now = _clock.elapsed;
    var outbound = outboundAt ?? now;
    if (outbound > now) {
      outbound = now;
    }
    _lastOutbound = outbound;
    _lastInbound = now;
    if (_running) {
      final due = _lastOutbound + keepAlive;
      _arm(due <= now ? now : due);
    }
  }

  /// Overrides the keep alive interval (server keep alive from CONNACK).
  ///
  /// The outbound timestamp is kept. Replacing it with "now" would push the
  /// first PINGREQ out by a full server keep-alive after a CONNECT that was
  /// already old.
  void updateKeepAlive(Duration keepAlive) {
    if (keepAlive == _keepAlive) {
      return;
    }
    final outbound = _lastOutbound;
    start(keepAlive, outboundAt: outbound);
  }

  void stop() {
    _running = false;
    _pingDeadline = null;
    _timer?.cancel();
    _timer = null;
  }

  /// Notes that bytes were written to the peer.
  void onOutboundActivity() {
    _lastOutbound = _clock.elapsed;
  }

  /// Notes that bytes arrived from the peer.
  void onInboundActivity() {
    _lastInbound = _clock.elapsed;
    if (_pingDeadline != null) {
      _pingDeadline = _lastInbound + effectivePingResponseTimeout;
    }
  }

  /// Notes that a PINGRESP arrived.
  void onPingResponse() {
    _pingDeadline = null;
  }

  void _arm(Duration at) {
    _timer?.cancel();
    final wait = at - _clock.elapsed;
    _timer = Timer(wait.isNegative ? Duration.zero : wait, _onTick);
  }

  void _onTick() {
    if (!_running) {
      return;
    }
    final now = _clock.elapsed;
    final deadline = _pingDeadline;
    if (deadline != null && now >= deadline) {
      _pingDeadline = null;
      _running = false;
      onPingTimeout();
      return;
    }
    final mustSend = now - _lastOutbound >= _keepAlive;
    final peerQuiet = deadline == null && now - _lastInbound >= _keepAlive;
    if (mustSend || peerQuiet) {
      _pingDeadline ??= now + effectivePingResponseTimeout;
      // The request counts as traffic whatever the callback does, so the
      // next one is a full interval away and a failing write cannot turn the
      // timer into a busy loop.
      _lastOutbound = now;
      onPingRequired();
      if (!_running) {
        return;
      }
    }
    final nextSend = _lastOutbound + _keepAlive;
    final watch = _pingDeadline ?? _lastInbound + _keepAlive;
    _arm(nextSend < watch ? nextSend : watch);
  }
}
