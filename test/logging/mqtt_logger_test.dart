import 'dart:async';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:mqtt5/testing.dart';
import 'package:test/test.dart';

void main() {
  group('PrintLogger', () {
    test('prints the configured level and more severe levels', () {
      final lines = <String>[];

      runZoned(
        () {
          final logger = PrintLogger(minimumLevel: MqttLogLevel.info);
          for (final level in MqttLogLevel.values) {
            logger.log(level, level.name);
          }
        },
        zoneSpecification: ZoneSpecification(
          print: (_, __, ___, line) => lines.add(line),
        ),
      );

      expect(lines, <String>[
        'mqtt5 [info] info',
        'mqtt5 [warning] warning',
        'mqtt5 [error] error',
      ]);
    });

    test('none disables all output', () {
      final lines = <String>[];

      runZoned(
        () => PrintLogger(minimumLevel: MqttLogLevel.none)
            .log(MqttLogLevel.error, 'hidden'),
        zoneSpecification: ZoneSpecification(
          print: (_, __, ___, line) => lines.add(line),
        ),
      );

      expect(lines, isEmpty);
    });
  });

  test('a logger exception cannot break connection handling', () async {
    final transport = MemoryTransport();
    final client = MqttClient(
      host: 'memory',
      clientId: 'throwing-logger',
      logger: const _ThrowingLogger(),
      transportFactory: () => transport,
    );

    final connecting = client.connect(keepAlive: Duration.zero);
    await Future<void>.delayed(Duration.zero);
    transport.inject(Uint8List.fromList(<int>[0x20, 0x03, 0x00, 0x00, 0x00]));

    await expectLater(connecting, completes);
    expect(client.state, MqttConnectionState.connected);
    await client.close();
  });
}

final class _ThrowingLogger implements MqttLogger {
  const _ThrowingLogger();

  @override
  void log(MqttLogLevel level, String message) {
    throw StateError('logger failed');
  }
}
