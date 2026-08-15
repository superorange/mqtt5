import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:mqtt5/testing.dart';
import 'package:test/test.dart';

Uint8List connackBytes({List<MqttProperty> properties = const []}) {
  return MqttPacketCodec.encode(
    MqttConnackPacket(sessionPresent: false, properties: properties),
  );
}

Future<MqttClient> connected(
  MemoryTransport transport, {
  List<MqttProperty> properties = const [],
  bool autoReconnect = true,
}) async {
  final client = MqttClient(
    host: 'x',
    transportFactory: () => transport,
    autoReconnect: autoReconnect,
  );
  final connecting = client.connect();
  await Future<void>.delayed(Duration.zero);
  transport.inject(connackBytes(properties: properties));
  await connecting;
  transport.takeOutgoing();
  return client;
}

List<MqttPacket> sentPackets(MemoryTransport transport) {
  final bytes = transport.takeOutgoingBytes();
  if (bytes.isEmpty) {
    return const [];
  }
  return MqttPacketDecoder().feed(bytes);
}

void main() {
  test('publish rejects wildcards in the topic name', () async {
    final transport = MemoryTransport();
    final client = await connected(transport);
    expect(
      () => client.publish('a/+/b', Uint8List(0)),
      throwsA(isA<ArgumentError>()),
    );
    expect(
      () => client.publish('a/#', Uint8List(0)),
      throwsA(isA<ArgumentError>()),
    );
  });

  test('connect rejects a keep alive above the protocol maximum', () async {
    final client = MqttClient(
      host: 'x',
      transportFactory: MemoryTransport.new,
    );
    await expectLater(
      client.connect(keepAlive: const Duration(days: 1)),
      throwsA(isA<ArgumentError>()),
    );
  });

  group('topic alias', () {
    test('a failed send does not leave a mapping the broker never saw',
        () async {
      final transport = MemoryTransport();
      final client = await connected(
        transport,
        properties: [TopicAliasMaximum(10), MaximumPacketSize(40)],
      );

      // Too large for the negotiated Maximum Packet Size, so the write fails
      // after an alias has been picked.
      await expectLater(
        client.publish('sensor/temperature', Uint8List(64)),
        throwsA(isA<MqttPacketTooLargeException>()),
      );
      transport.takeOutgoing();

      // The next publish to the same topic must still carry the full name.
      await client.publish('sensor/temperature', Uint8List(0));
      final publish = sentPackets(transport).single as MqttPublishPacket;
      expect(publish.topicName, 'sensor/temperature');
    });

    test('a successful send binds the alias for later publishes', () async {
      final transport = MemoryTransport();
      final client = await connected(
        transport,
        properties: [TopicAliasMaximum(10)],
      );

      await client.publish('sensor/temperature', Uint8List(0));
      final first = sentPackets(transport).single as MqttPublishPacket;
      expect(first.topicName, 'sensor/temperature');
      expect(first.properties.whereType<TopicAlias>().single.value, 1);

      await client.publish('sensor/temperature', Uint8List(0));
      final second = sentPackets(transport).single as MqttPublishPacket;
      expect(second.topicName, isEmpty);
      expect(second.properties.whereType<TopicAlias>().single.value, 1);
    });
  });

  group('SUBACK handling', () {
    test('a reason code count mismatch is a protocol error', () async {
      final transport = MemoryTransport();
      final client = await connected(transport);

      final pending = client.subscribeAll([
        const MqttSubscription('a/1'),
        const MqttSubscription('a/2'),
      ]);
      await Future<void>.delayed(Duration.zero);
      final subscribe = sentPackets(transport).single as MqttSubscribePacket;
      transport.inject(
        MqttPacketCodec.encode(
          MqttSubackPacket(
            packetIdentifier: subscribe.packetIdentifier,
            reasonCodes: const [0x00],
          ),
        ),
      );

      await expectLater(pending, throwsA(isA<MqttProtocolException>()));
    });

    test('filters accepted alongside a rejected one are still tracked',
        () async {
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
      transports.last.inject(connackBytes());
      await connecting;
      transports.last.takeOutgoing();

      final pending = client.subscribeAll([
        const MqttSubscription('a/1'),
        const MqttSubscription('a/2'),
      ]);
      await Future<void>.delayed(Duration.zero);
      final subscribe =
          sentPackets(transports.last).single as MqttSubscribePacket;
      transports.last.inject(
        MqttPacketCodec.encode(
          MqttSubackPacket(
            packetIdentifier: subscribe.packetIdentifier,
            reasonCodes: const [0x00, 0x87],
          ),
        ),
      );
      await expectLater(
        pending,
        throwsA(isA<MqttServerRejectedException>()),
      );

      // Reconnecting without a session must re-subscribe to the accepted
      // filter only.
      transports.last.injectError(MqttTransportException('boom'));
      await Future<void>.delayed(const Duration(milliseconds: 30));
      transports.last.takeOutgoing();
      transports.last.inject(connackBytes());
      await Future<void>.delayed(const Duration(milliseconds: 30));

      final resubscribe = sentPackets(transports.last)
          .whereType<MqttSubscribePacket>()
          .single;
      expect(
        resubscribe.subscriptions.map((s) => s.topicFilter),
        ['a/1'],
      );
    });
  });

  test('a broker DISCONNECT tears the connection down', () async {
    final transport = MemoryTransport();
    final client = await connected(transport, autoReconnect: false);

    transport.inject(
      MqttPacketCodec.encode(
        const MqttDisconnectPacket(
          reasonCode: MqttReasonCode.serverShuttingDown,
        ),
      ),
    );
    await Future<void>.delayed(const Duration(milliseconds: 20));

    expect(transport.isClosed, isTrue);
    expect(client.state, MqttConnectionState.disconnected);
  });

  test('a redirecting DISCONNECT stops the client and reports the reference',
      () async {
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
    String? reference;
    client.onServerMoved = (serverReference, _) => reference = serverReference;
    final errors = <Object>[];
    client.errors.listen(errors.add);

    final connecting = client.connect();
    await Future<void>.delayed(Duration.zero);
    transports.last.inject(connackBytes());
    await connecting;

    transports.last.inject(
      MqttPacketCodec.encode(
        const MqttDisconnectPacket(
          reasonCode: MqttReasonCode.serverMoved,
          properties: [ServerReference('broker2.example.com:1883')],
        ),
      ),
    );
    await Future<void>.delayed(const Duration(milliseconds: 50));

    expect(reference, 'broker2.example.com:1883');
    expect(transports, hasLength(1), reason: 'must not retry the old server');
    expect(errors, [isA<MqttServerMovedException>()]);
  });
}
