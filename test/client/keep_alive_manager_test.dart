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
}

Future<void> _delay(Duration duration) => Future<void>.delayed(duration);
