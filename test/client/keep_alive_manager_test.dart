import 'package:mqtt5/src/client/keep_alive_manager.dart';
import 'package:test/test.dart';

void main() {
  group('KeepAliveManager', () {
    test('sends ping after idle interval', () async {
      var pings = 0;
      var timeouts = 0;
      final manager = KeepAliveManager(
        onPingRequired: () => pings++,
        onPingTimeout: () => timeouts++,
      );
      manager.start(const Duration(milliseconds: 30));
      await _delay(const Duration(milliseconds: 40));
      expect(pings, 1);
      expect(timeouts, 0);
      manager.stop();
    });

    test('pings again after response', () async {
      var pings = 0;
      final manager = KeepAliveManager(
        onPingRequired: () => pings++,
        onPingTimeout: () {},
      );
      manager.start(const Duration(milliseconds: 30));
      await _delay(const Duration(milliseconds: 45));
      expect(pings, 1);
      manager.onPingResponse();
      await _delay(const Duration(milliseconds: 45));
      expect(pings, 2);
      manager.stop();
    });

    test('times out when no ping response arrives', () async {
      var pings = 0;
      var timeouts = 0;
      final manager = KeepAliveManager(
        onPingRequired: () => pings++,
        onPingTimeout: () => timeouts++,
      );
      manager.start(const Duration(milliseconds: 30));
      await _delay(const Duration(milliseconds: 100));
      expect(pings, greaterThanOrEqualTo(1));
      expect(timeouts, greaterThanOrEqualTo(1));
      manager.stop();
    });

    test('outbound activity postpones the ping', () async {
      var pings = 0;
      final manager = KeepAliveManager(
        onPingRequired: () => pings++,
        onPingTimeout: () {},
      );
      manager.start(const Duration(milliseconds: 40));
      // Keep pushing the timer out before it fires.
      for (var i = 0; i < 6; i++) {
        await _delay(const Duration(milliseconds: 20));
        manager.onOutboundActivity();
      }
      expect(pings, 0);
      manager.stop();
    });

    test('zero keep alive disables pings', () async {
      var pings = 0;
      final manager = KeepAliveManager(
        onPingRequired: () => pings++,
        onPingTimeout: () {},
      );
      manager.start(Duration.zero);
      await _delay(const Duration(milliseconds: 80));
      expect(pings, 0);
      expect(manager.isRunning, isFalse);
    });
  });

  group('pingResponseTimeout', () {
    test('defaults to the keep alive interval', () {
      final manager = KeepAliveManager(
        onPingRequired: () {},
        onPingTimeout: () {},
      );
      manager.start(const Duration(seconds: 30));
      expect(manager.pingResponseTimeout, isNull);
      expect(manager.effectivePingResponseTimeout, const Duration(seconds: 30));
      manager.stop();
    });

    test('a short timeout detects a dead link inside one keep alive', () async {
      var pings = 0;
      var timeouts = 0;
      final manager = KeepAliveManager(
        onPingRequired: () => pings++,
        onPingTimeout: () => timeouts++,
        pingResponseTimeout: const Duration(milliseconds: 20),
      );
      // Idle for 60 ms, then only 20 ms for the answer instead of another 60.
      manager.start(const Duration(milliseconds: 60));
      await _delay(const Duration(milliseconds: 95));
      expect(pings, 1);
      expect(
        timeouts,
        1,
        reason: 'the PINGRESP deadline is pingResponseTimeout, not keepAlive',
      );
      manager.stop();
    });

    test('the default would not have timed out that early', () async {
      var pings = 0;
      var timeouts = 0;
      final manager = KeepAliveManager(
        onPingRequired: () => pings++,
        onPingTimeout: () => timeouts++,
      );
      manager.start(const Duration(milliseconds: 60));
      await _delay(const Duration(milliseconds: 95));
      expect(pings, 1);
      expect(timeouts, 0, reason: 'still inside the second keep alive interval');
      manager.stop();
    });

    test('a PINGRESP inside the window cancels the timeout', () async {
      var timeouts = 0;
      var pings = 0;
      final manager = KeepAliveManager(
        onPingRequired: () => pings++,
        onPingTimeout: () => timeouts++,
        pingResponseTimeout: const Duration(milliseconds: 40),
      );
      manager.start(const Duration(milliseconds: 30));
      await _delay(const Duration(milliseconds: 45));
      expect(pings, 1);
      manager.onPingResponse();
      // The response window would have expired 40 ms after the PINGREQ; the
      // reply cancelled it, and the idle timer is back to the 30 ms keep alive.
      await _delay(const Duration(milliseconds: 50));
      expect(timeouts, 0);
      expect(pings, 2);
      manager.stop();
    });

    test('a non-positive timeout is rejected', () {
      for (final bad in [Duration.zero, const Duration(seconds: -1)]) {
        expect(
          () => KeepAliveManager(
            onPingRequired: () {},
            onPingTimeout: () {},
            pingResponseTimeout: bad,
          ),
          throwsA(isA<ArgumentError>()),
          reason: '$bad',
        );
      }
    });
  });
}

Future<void> _delay(Duration duration) => Future<void>.delayed(duration);
