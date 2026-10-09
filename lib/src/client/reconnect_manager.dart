import 'dart:math';

/// How the next reconnect should be paced after a connection ends.
enum ReconnectPace {
  /// The connection had been up for [ReconnectManager.stableAfter] and the
  /// peer did not close it. Reconnect at once; the backoff attempt counter
  /// has been reset.
  immediate,

  /// The connection survived [ReconnectManager.flapWindow] but not
  /// [ReconnectManager.stableAfter], and the peer did not close it. Wait one
  /// jittered [ReconnectManager.initialDelay]. The attempt counter has been
  /// reset, so the wait does not climb.
  once,

  /// Keep the exponential sequence. Used for an incomplete handshake, a
  /// server-initiated DISCONNECT, and a connection that died inside
  /// [ReconnectManager.flapWindow].
  escalate,
}

/// Exponential backoff with jitter for reconnect attempts.
final class ReconnectManager {
  ReconnectManager({
    this.initialDelay = const Duration(seconds: 1),
    this.maxDelay = const Duration(seconds: 30),
    this.jitterFactor = 0.2,
    this.flapWindow = const Duration(seconds: 2),
    this.stableAfter = const Duration(seconds: 10),
    Random? random,
  }) : _random = random ?? Random() {
    if (initialDelay < Duration.zero) {
      throw ArgumentError.value(
        initialDelay,
        'initialDelay',
        'Must not be negative',
      );
    }
    if (maxDelay < initialDelay) {
      throw ArgumentError.value(
        maxDelay,
        'maxDelay',
        'Must be greater than or equal to initialDelay',
      );
    }
    if (flapWindow < Duration.zero) {
      throw ArgumentError.value(
        flapWindow,
        'flapWindow',
        'Must not be negative',
      );
    }
    if (stableAfter < Duration.zero) {
      throw ArgumentError.value(
        stableAfter,
        'stableAfter',
        'Must not be negative',
      );
    }
    if (jitterFactor < 0 || jitterFactor > 1) {
      throw ArgumentError.value(
        jitterFactor,
        'jitterFactor',
        'Must be between 0 and 1',
      );
    }
  }

  final Duration initialDelay;
  final Duration maxDelay;
  final double jitterFactor;

  /// A connection that ends before this, after the handshake, is treated as
  /// an accept-and-drop streak: the backoff keeps climbing.
  ///
  /// Zero disables that streak. Every completed connection is then paced as
  /// [ReconnectPace.once] or [ReconnectPace.immediate]. [stableAfter] is
  /// checked first, so the part of this window at or above it has no effect.
  final Duration flapWindow;

  /// How long a connection must last before a network drop is treated as a
  /// fresh failure (reconnect at once, backoff reset). A server-initiated
  /// DISCONNECT still waits, but the wait starts again at [initialDelay].
  ///
  /// Without a gap between [flapWindow] and this value, a server that accepts
  /// and then drops the connection would be reconnected to in a tight loop.
  final Duration stableAfter;

  final Random _random;

  int _attempt = 0;

  int get attempt => _attempt;

  void reset() {
    _attempt = 0;
  }

  /// Chooses the pace for a connection that just ended and resets the attempt
  /// counter when this loss should not continue a streak.
  ///
  /// [lived] is how long the connection stayed up after the handshake. Pass
  /// [Duration.zero] when the handshake never finished. [serverInitiated] is
  /// true when the broker sent DISCONNECT.
  ReconnectPace paceFor({
    required bool handshakeComplete,
    required bool serverInitiated,
    required Duration lived,
  }) {
    if (!handshakeComplete || serverInitiated) {
      // A stable connection the broker closed still backs off, but from the
      // initial delay: the previous streak is over, and reconnecting at once
      // would fight a deliberate close.
      if (handshakeComplete && lived >= stableAfter) {
        reset();
      }
      return ReconnectPace.escalate;
    }
    if (lived >= stableAfter) {
      reset();
      return ReconnectPace.immediate;
    }
    if (lived >= flapWindow) {
      reset();
      return ReconnectPace.once;
    }
    return ReconnectPace.escalate;
  }

  /// Returns the delay to wait before the next reconnect attempt.
  Duration nextDelay() {
    final attempt = _attempt;
    _attempt++;
    final base = _exponentialDelay(attempt);
    return _applyJitter(base);
  }

  Duration _exponentialDelay(int attempt) {
    var seconds = initialDelay.inMilliseconds;
    for (var i = 0; i < attempt && seconds < maxDelay.inMilliseconds; i++) {
      seconds *= 2;
      if (seconds > maxDelay.inMilliseconds) {
        seconds = maxDelay.inMilliseconds;
        break;
      }
    }
    return Duration(milliseconds: seconds);
  }

  Duration _applyJitter(Duration base) {
    final jitterRange = (base.inMilliseconds * jitterFactor).round();
    final delta = _random.nextInt(jitterRange * 2 + 1) - jitterRange;
    final jittered = base.inMilliseconds + delta;
    final min = (initialDelay.inMilliseconds * 0.5).round();
    return Duration(milliseconds: max(min, jittered));
  }
}
