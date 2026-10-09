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

    test('rejects invalid backoff configuration eagerly', () {
      expect(
        () => ReconnectManager(initialDelay: const Duration(milliseconds: -1)),
        throwsArgumentError,
      );
      expect(
        () => ReconnectManager(
          initialDelay: const Duration(seconds: 2),
          maxDelay: const Duration(seconds: 1),
        ),
        throwsArgumentError,
      );
      expect(
        () => ReconnectManager(jitterFactor: -0.1),
        throwsArgumentError,
      );
      expect(
        () => ReconnectManager(jitterFactor: 1.1),
        throwsArgumentError,
      );
      expect(
        () => ReconnectManager(flapWindow: const Duration(milliseconds: -1)),
        throwsArgumentError,
      );
      expect(
        () => ReconnectManager(stableAfter: const Duration(milliseconds: -1)),
        throwsArgumentError,
      );
    });

    test('paceFor classifies handshake, flaps and stable drops', () {
      final manager = ReconnectManager(
        flapWindow: const Duration(seconds: 2),
        stableAfter: const Duration(seconds: 10),
      );

      expect(
        manager.paceFor(
          handshakeComplete: false,
          serverInitiated: false,
          lived: Duration.zero,
        ),
        ReconnectPace.escalate,
      );
      expect(manager.attempt, 0);

      manager.nextDelay();
      expect(
        manager.paceFor(
          handshakeComplete: true,
          serverInitiated: false,
          lived: const Duration(milliseconds: 500),
        ),
        ReconnectPace.escalate,
      );
      expect(manager.attempt, 1);

      expect(
        manager.paceFor(
          handshakeComplete: true,
          serverInitiated: false,
          lived: const Duration(seconds: 3),
        ),
        ReconnectPace.once,
      );
      expect(manager.attempt, 0);

      manager.nextDelay();
      expect(
        manager.paceFor(
          handshakeComplete: true,
          serverInitiated: false,
          lived: const Duration(seconds: 10),
        ),
        ReconnectPace.immediate,
      );
      expect(manager.attempt, 0);

      manager.nextDelay();
      manager.nextDelay();
      expect(
        manager.paceFor(
          handshakeComplete: true,
          serverInitiated: true,
          lived: const Duration(seconds: 10),
        ),
        ReconnectPace.escalate,
      );
      expect(manager.attempt, 0);

      manager.nextDelay();
      expect(
        manager.paceFor(
          handshakeComplete: true,
          serverInitiated: true,
          lived: const Duration(milliseconds: 100),
        ),
        ReconnectPace.escalate,
      );
      expect(manager.attempt, 1);
    });
  });
}
