import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:mqtt5/testing.dart';
import 'package:test/test.dart';

void main() {
  group('connection resilience', () {
    test('transport error during handshake fails immediately', () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'x',
        autoReconnect: false,
        transportFactory: () => transport,
      );

      final connecting = client.connect(
        connackTimeout: const Duration(seconds: 10),
      );
      await Future<void>.delayed(Duration.zero);
      transport.injectError(TlsException('TLSV1_ALERT_UNKNOWN_CA'));

      await expectLater(
        connecting.timeout(const Duration(milliseconds: 500)),
        throwsA(isA<TlsException>()),
      );
      expect(client.state, MqttConnectionState.disconnected);
      await client.close();
    });

    test('disabled reconnect reports an established connection loss', () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'x',
        autoReconnect: false,
        transportFactory: () => transport,
      );
      await _handshake(client, transport);

      final failure = MqttTransportException('connection reset');
      final reported = client.errors.first;
      final structured = client.errorEvents.first;
      transport.injectError(failure);

      expect(
        await reported.timeout(const Duration(milliseconds: 500)),
        same(failure),
      );
      final event = await structured.timeout(const Duration(milliseconds: 500));
      expect(event.error, same(failure));
      expect(event.stackTrace.toString(), isNotEmpty);
      expect(client.state, MqttConnectionState.disconnected);
      await client.close();
    });

    test('TLS handshake failure on reconnect is terminal', () async {
      final first = MemoryTransport();
      var attempts = 0;
      final client = MqttClient(
        host: 'x',
        transportFactory: () {
          attempts++;
          return attempts == 1
              ? first
              : _ConnectFailureTransport(
                  TlsException('TLSV1_ALERT_UNKNOWN_CA'),
                );
        },
        reconnectManager: ReconnectManager(
          initialDelay: const Duration(milliseconds: 1),
          maxDelay: const Duration(milliseconds: 1),
          jitterFactor: 0,
        ),
      );
      await _handshake(client, first);

      final reported = client.errors.first;
      first.injectError(MqttTransportException('connection lost'));

      expect(
        await reported.timeout(const Duration(milliseconds: 500)),
        isA<TlsException>(),
      );
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(attempts, 2, reason: 'certificate failures must not loop forever');
      expect(client.state, MqttConnectionState.disconnected);
      await client.close();
    });

    test('disconnect cancels retry delay and permits a fresh connect',
        () async {
      final recovered = MemoryTransport();
      var attempts = 0;
      final client = MqttClient(
        host: 'x',
        transportFactory: () {
          attempts++;
          return attempts == 1
              ? _ConnectFailureTransport(
                  const SocketException('refused'),
                )
              : recovered;
        },
        reconnectManager: ReconnectManager(
          initialDelay: const Duration(seconds: 5),
          maxDelay: const Duration(seconds: 5),
          jitterFactor: 0,
        ),
      );

      final firstConnect = client.connect();
      await _waitFor(() => attempts == 1);
      final disconnecting =
          client.disconnect().timeout(const Duration(milliseconds: 500));
      await expectLater(firstConnect, throwsA(isA<SocketException>()));
      await disconnecting;

      final secondConnect = client.connect();
      await _waitFor(() => attempts == 2);
      recovered.inject(_connackBytes());
      await secondConnect.timeout(const Duration(milliseconds: 500));

      expect(client.state, MqttConnectionState.connected);
      await client.close();
    });

    test('transport close failure does not escape the background task',
        () async {
      final transport = _CloseFailureTransport();
      final uncaught = <Object>[];

      await runZonedGuarded(() async {
        final client = MqttClient(
          host: 'x',
          autoReconnect: false,
          transportFactory: () => transport,
        );
        final connecting = client.connect();
        await Future<void>.delayed(Duration.zero);
        transport.inject(_connackBytes());
        await connecting;
        final reported = client.errors.first;
        transport.injectError(MqttTransportException('connection lost'));
        await reported.timeout(const Duration(milliseconds: 500));
        await Future<void>.delayed(const Duration(milliseconds: 20));
      }, (error, stackTrace) => uncaught.add(error));

      expect(uncaught, isEmpty);
    });

    test('invalid CONNECT configuration fails without retrying', () async {
      var attempts = 0;
      final client = MqttClient(
        host: 'x',
        transportFactory: () {
          attempts++;
          return MemoryTransport();
        },
        reconnectManager: ReconnectManager(
          initialDelay: const Duration(milliseconds: 1),
          maxDelay: const Duration(milliseconds: 1),
          jitterFactor: 0,
        ),
      );

      await expectLater(
        client.connect(receiveMaximum: 0),
        throwsArgumentError,
      );
      expect(attempts, 0, reason: 'local validation must run before I/O');
      await client.close();
    });

    test('fatal disconnect reason reaches pending operations', () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'x',
        transportFactory: () => transport,
      );
      await _handshake(client, transport);

      final pending = expectLater(
        client.subscribe('a/#'),
        throwsA(
          isA<MqttServerRejectedException>()
              .having(
                (error) => error.reasonCode,
                'reasonCode',
                MqttReasonCode.notAuthorized.value,
              )
              .having(
                (error) => error.message,
                'diagnostic message',
                allOf(
                  contains('notAuthorized code=0x87'),
                  contains('reasonString="policy revoked"'),
                  contains('traceId=abc123'),
                ),
              ),
        ),
      );
      await Future<void>.delayed(Duration.zero);
      transport.inject(
        MqttPacketCodec.encode(
          const MqttDisconnectPacket(
            reasonCode: MqttReasonCode.notAuthorized,
            properties: [
              ReasonString('policy revoked'),
              UserProperty('traceId', 'abc123'),
            ],
          ),
        ),
      );

      await pending.timeout(const Duration(milliseconds: 500));
      await client.close();
    });
  });
}

Future<void> _handshake(MqttClient client, MemoryTransport transport) async {
  final connecting = client.connect();
  await Future<void>.delayed(Duration.zero);
  transport.inject(_connackBytes());
  await connecting;
}

Uint8List _connackBytes() => MqttPacketCodec.encode(
      const MqttConnackPacket(sessionPresent: false),
    );

Future<void> _waitFor(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 1));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('Timed out waiting for condition');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

final class _ConnectFailureTransport implements MqttTransport {
  _ConnectFailureTransport(this.failure);

  final Object failure;

  @override
  Stream<Uint8List> get incoming => const Stream<Uint8List>.empty();

  @override
  bool get isConnected => false;

  @override
  void add(Uint8List data) {}

  @override
  Future<void> close() async {}

  @override
  Future<void> connect() async => throw failure;

  @override
  Future<void> flush() async {}
}

final class _CloseFailureTransport implements MqttTransport {
  final StreamController<Uint8List> _incoming =
      StreamController<Uint8List>.broadcast(sync: true);
  bool _connected = false;

  void inject(Uint8List data) => _incoming.add(data);

  void injectError(Object error) => _incoming.addError(error);

  @override
  Stream<Uint8List> get incoming => _incoming.stream;

  @override
  bool get isConnected => _connected;

  @override
  void add(Uint8List data) {}

  @override
  Future<void> close() async {
    _connected = false;
    await _incoming.close();
    throw MqttTransportException('close failed');
  }

  @override
  Future<void> connect() async => _connected = true;

  @override
  Future<void> flush() async {}
}
