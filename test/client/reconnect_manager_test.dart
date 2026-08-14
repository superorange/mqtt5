import 'dart:math';

import 'package:mqtt5/src/client/reconnect_manager.dart';
import 'package:test/test.dart';

void main() {
  group('ReconnectManager', () {
    test('doubles delay up to max', () {
      final manager = ReconnectManager(
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(seconds: 30),
        jitterFactor: 0.0,
        random: Random(0),
      );
      expect(manager.nextDelay(), const Duration(seconds: 1));
      expect(manager.nextDelay(), const Duration(seconds: 2));
      expect(manager.nextDelay(), const Duration(seconds: 4));
      expect(manager.nextDelay(), const Duration(seconds: 8));
      expect(manager.nextDelay(), const Duration(seconds: 16));
      expect(manager.nextDelay(), const Duration(seconds: 30));
      expect(manager.nextDelay(), const Duration(seconds: 30));
    });

    test('jitter stays within bounds', () {
      final manager = ReconnectManager(
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(seconds: 30),
        jitterFactor: 0.2,
        random: Random(42),
      );
      for (var i = 0; i < 100; i++) {
        final delay = manager.nextDelay();
        expect(delay.inMilliseconds, greaterThanOrEqualTo(500));
        expect(delay.inMilliseconds, lessThanOrEqualTo(36000));
      }
    });

    test('reset restarts the sequence', () {
      final manager = ReconnectManager(
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(seconds: 30),
        jitterFactor: 0.0,
        random: Random(0),
      );
      manager.nextDelay();
      manager.nextDelay();
      manager.reset();
      expect(manager.nextDelay(), const Duration(seconds: 1));
    });
  });
}
