import 'dart:math';

/// Exponential backoff with jitter for reconnect attempts.
final class ReconnectManager {
  ReconnectManager({
    this.initialDelay = const Duration(seconds: 1),
    this.maxDelay = const Duration(seconds: 30),
    this.jitterFactor = 0.2,
    Random? random,
  }) : _random = random ?? Random();

  final Duration initialDelay;
  final Duration maxDelay;
  final double jitterFactor;
  final Random _random;

  int _attempt = 0;

  int get attempt => _attempt;

  void reset() {
    _attempt = 0;
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
