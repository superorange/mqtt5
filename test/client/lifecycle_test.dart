import 'dart:async';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:mqtt5/testing.dart';
import 'package:test/test.dart';

Uint8List connackBytes({
  bool sessionPresent = false,
  MqttReasonCode reasonCode = MqttReasonCode.success,
  List<MqttProperty> properties = const [],
}) {
  return MqttPacketCodec.encode(
    MqttConnackPacket(
      sessionPresent: sessionPresent,
      reasonCode: reasonCode,
      properties: properties,
    ),
  );
}

/// Connects [client] against [transport] by feeding it a CONNACK.
Future<void> handshake(
  MqttClient client,
  MemoryTransport Function() current, {
  bool sessionPresent = false,
  List<MqttProperty> properties = const [],
}) async {
  final connecting = client.connect(cleanStart: !sessionPresent);
  await Future<void>.delayed(Duration.zero);
  current().inject(
    connackBytes(sessionPresent: sessionPresent, properties: properties),
  );
  await connecting;
}

void main() {
  group('teardown', () {
    test('disconnect before connect does not throw', () async {
      final client = MqttClient(
        host: 'localhost',
        transportFactory: MemoryTransport.new,
      );
      await client.disconnect();
      expect(client.state, MqttConnectionState.disconnected);
    });

    test('disconnect fails publishes that are still awaiting an ack', () async {
      final transport = MemoryTransport();
      final client = MqttClient(host: 'x', transportFactory: () => transport);
      await handshake(client, () => transport);

      final pending = client.publish(
        'a/b',
        Uint8List.fromList([1]),
        qos: MqttQos.atLeastOnce,
      );
      await Future<void>.delayed(Duration.zero);
      await client.disconnect();

      await expectLater(
        pending.timeout(const Duration(seconds: 2)),
        throwsA(isA<MqttConnectionException>()),
      );
    });

    test('disconnect fails subscribes that are still awaiting an ack',
        () async {
      final transport = MemoryTransport();
      final client = MqttClient(host: 'x', transportFactory: () => transport);
      await handshake(client, () => transport);

      final pending = client.subscribe('a/#');
      await Future<void>.delayed(Duration.zero);
      await client.disconnect();

      await expectLater(
        pending.timeout(const Duration(seconds: 2)),
        throwsA(isA<MqttConnectionException>()),
      );
    });

    test('close releases the streams', () async {
      final transport = MemoryTransport();
      final client = MqttClient(host: 'x', transportFactory: () => transport);
      await handshake(client, () => transport);

      final drained = client.messages.toList();
      await client.close();
      expect(await drained, isEmpty);
      await expectLater(
        client.connect(),
        throwsA(isA<MqttConnectionException>()),
      );
    });
  });

  group('fatal connection errors', () {
    test('a rejected CONNACK leaves the client usable for a retry', () async {
      final transports = <MemoryTransport>[];
      final client = MqttClient(
        host: 'x',
        transportFactory: () {
          final transport = MemoryTransport();
          transports.add(transport);
          return transport;
        },
      );

      final rejected = client.connect();
      await Future<void>.delayed(Duration.zero);
      transports.last.inject(
        connackBytes(reasonCode: MqttReasonCode.badUserNameOrPassword),
      );
      await expectLater(rejected, throwsA(isA<MqttServerRejectedException>()));

      // The failed attempt must not leak its transport...
      expect(transports.single.isConnected, isFalse);
      expect(transports.single.isClosed, isTrue);
      expect(client.state, MqttConnectionState.disconnected);

      // ...and a second attempt must really reconnect rather than no-op.
      final retry = client.connect();
      await Future<void>.delayed(Duration.zero);
      expect(transports, hasLength(2));
      transports.last.inject(connackBytes());
      await retry;
      expect(client.state, MqttConnectionState.connected);
    });

    test('a rejection on reconnect is reported instead of going unhandled',
        () async {
      final transports = <MemoryTransport>[];
      final errors = <Object>[];
      final uncaught = <Object>[];

      await runZonedGuarded(() async {
        final client = MqttClient(
          host: 'x',
          transportFactory: () {
            final transport = MemoryTransport();
            transports.add(transport);
            return transport;
          },
          reconnectManager: ReconnectManager(
            initialDelay: const Duration(milliseconds: 1),
          ),
        );
        client.errors.listen(errors.add);
        await handshake(client, () => transports.last);

        transports.last.injectError(MqttTransportException('boom'));
        await Future<void>.delayed(const Duration(milliseconds: 50));
        transports.last
            .inject(connackBytes(reasonCode: MqttReasonCode.notAuthorized));
        await Future<void>.delayed(const Duration(milliseconds: 50));
      }, (error, stack) => uncaught.add(error));

      expect(uncaught, isEmpty);
      expect(errors, [isA<MqttServerRejectedException>()]);
    });
  });

  group('autoReconnect', () {
    test('false stops the client after the connection drops', () async {
      final transports = <MemoryTransport>[];
      final client = MqttClient(
        host: 'x',
        autoReconnect: false,
        transportFactory: () {
          final transport = MemoryTransport();
          transports.add(transport);
          return transport;
        },
      );
      await handshake(client, () => transports.last);

      transports.last.injectError(MqttTransportException('boom'));
      await Future<void>.delayed(const Duration(milliseconds: 50));

      expect(transports, hasLength(1), reason: 'must not have reconnected');
      expect(client.state, MqttConnectionState.disconnected);
    });

    test('false makes a failed first connect throw instead of retrying',
        () async {
      var attempts = 0;
      final client = MqttClient(
        host: 'x',
        autoReconnect: false,
        transportFactory: () {
          attempts++;
          return _FailingTransport();
        },
      );
      await expectLater(client.connect(), throwsA(isA<MqttException>()));
      expect(attempts, 1);
    });

    test('true reconnects after the connection drops', () async {
      final transports = <MemoryTransport>[];
      final client = MqttClient(
        host: 'x',
        transportFactory: () {
          final transport = MemoryTransport();
          transports.add(transport);
          return transport;
        },
        reconnectManager: ReconnectManager(
          initialDelay: const Duration(milliseconds: 1),
        ),
      );
      await handshake(client, () => transports.last);

      transports.last.injectError(MqttTransportException('boom'));
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(transports, hasLength(2));
    });
  });

  group('operation timeout', () {
    test('publish fails when the broker never acknowledges', () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'x',
        transportFactory: () => transport,
        operationTimeout: const Duration(milliseconds: 50),
      );
      await handshake(client, () => transport);

      await expectLater(
        client.publish('a/b', Uint8List(0), qos: MqttQos.atLeastOnce),
        throwsA(isA<MqttTimeoutException>()),
      );
      // The PUBLISH is already on the wire, so session state stays until
      // PUBACK, session discard, or disconnect.
      expect(client.inflightCount, 1);
    });

    test('subscribe fails when the broker never acknowledges', () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'x',
        transportFactory: () => transport,
        operationTimeout: const Duration(milliseconds: 50),
      );
      await handshake(client, () => transport);

      await expectLater(
        client.subscribe('a/#'),
        throwsA(isA<MqttTimeoutException>()),
      );
    });
  });

  test('server capabilities do not leak across connections', () async {
    final transports = <MemoryTransport>[];
    final client = MqttClient(
      host: 'x',
      transportFactory: () {
        final transport = MemoryTransport();
        transports.add(transport);
        return transport;
      },
      reconnectManager: ReconnectManager(
        initialDelay: const Duration(milliseconds: 1),
      ),
    );

    final connecting = client.connect();
    await Future<void>.delayed(Duration.zero);
    transports.last.inject(
      connackBytes(properties: [MaximumQos(0), RetainAvailable(0)]),
    );
    await connecting;

    expect(
      () => client.publish('a/b', Uint8List(0), qos: MqttQos.atLeastOnce),
      throwsA(isA<MqttFlowControlException>()),
    );

    transports.last.injectError(MqttTransportException('boom'));
    await Future<void>.delayed(const Duration(milliseconds: 50));
    // The new CONNACK omits both properties, so the defaults apply again.
    transports.last.inject(connackBytes());
    await Future<void>.delayed(const Duration(milliseconds: 20));

    expect(
      client.publish('a/b', Uint8List(0), qos: MqttQos.atLeastOnce),
      isA<Future<MqttPublishResult>>(),
    );
    expect(
      () => client.publish('a/b', Uint8List(0), retain: true),
      returnsNormally,
    );
  });

  test('the broker-assigned client identifier is reused on reconnect',
      () async {
    final transports = <MemoryTransport>[];
    final client = MqttClient(
      host: 'x',
      clientId: '',
      transportFactory: () {
        final transport = MemoryTransport();
        transports.add(transport);
        return transport;
      },
      reconnectManager: ReconnectManager(
        initialDelay: const Duration(milliseconds: 1),
      ),
    );

    final connecting = client.connect();
    await Future<void>.delayed(Duration.zero);
    transports.last.takeOutgoing();
    transports.last.inject(
      connackBytes(properties: [const AssignedClientIdentifier('broker-42')]),
    );
    await connecting;
    expect(client.effectiveClientId, 'broker-42');

    transports.last.injectError(MqttTransportException('boom'));
    await Future<void>.delayed(const Duration(milliseconds: 50));

    final sent = MqttPacketCodec.decode(transports.last.takeOutgoingBytes())
        as MqttConnectPacket;
    expect(sent.clientId, 'broker-42');
  });
}

final class _FailingTransport implements MqttTransport {
  @override
  Stream<Uint8List> get incoming => const Stream<Uint8List>.empty();

  @override
  Future<void> connect() async {
    throw MqttTransportException('refused');
  }

  @override
  void add(Uint8List data) {}

  @override
  Future<void> flush() async {}

  @override
  Future<void> close() async {}

  @override
  bool get isConnected => false;
}
