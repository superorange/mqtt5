import 'dart:typed_data';

import 'package:mqtt5/src/client/mqtt_client.dart';
import 'package:mqtt5/src/packet/connack.dart';
import 'package:mqtt5/src/packet/mqtt_packet.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/packet/publish.dart';
import 'package:mqtt5/src/packet/suback.dart';
import 'package:mqtt5/src/packet/subscribe.dart';
import 'package:mqtt5/src/property/mqtt_property.dart';
import 'package:mqtt5/src/transport/memory_transport.dart';
import 'package:test/test.dart';

void main() {
  test('incoming topic alias is resolved', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect(topicAliasMaximum: 10);
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: false),
      ),
    );
    await connectFuture;

    final messages = <dynamic>[];
    final sub = client.messages.listen(messages.add);

    // Establish alias 1 -> real/topic.
    transport.inject(
      MqttPacketCodec.encode(
        MqttPublishPacket(
          topicName: 'real/topic',
          payload: Uint8List.fromList([1]),
          properties: const [TopicAlias(1)],
        ),
      ),
    );
    // Use alias with empty topic.
    transport.inject(
      MqttPacketCodec.encode(
        MqttPublishPacket(
          topicName: '',
          payload: Uint8List.fromList([2]),
          properties: const [TopicAlias(1)],
        ),
      ),
    );

    await _waitFor(() => messages.length == 2);
    expect(messages[0].topic, 'real/topic');
    expect(messages[1].topic, 'real/topic');
    expect(messages[1].payload, [2]);

    await sub.cancel();
    await client.disconnect();
  });

  test('outgoing topic alias is assigned and reused', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          properties: [TopicAliasMaximum(5)],
        ),
      ),
    );
    await connectFuture;

    await client.publish('device/foo', Uint8List.fromList([1]));
    await client.publish('device/foo', Uint8List.fromList([2]));

    final chunks = transport.takeOutgoing();
    expect(chunks, hasLength(2));
    final first = MqttPacketCodec.decode(chunks[0]) as MqttPublishPacket;
    final second = MqttPacketCodec.decode(chunks[1]) as MqttPublishPacket;

    expect(first.topicName, 'device/foo');
    expect(first.properties, contains(isA<TopicAlias>()));
    final alias = first.properties.whereType<TopicAlias>().single.value;

    expect(second.topicName, isEmpty);
    expect(second.properties.whereType<TopicAlias>().single.value, alias);

    await client.disconnect();
  });

  test('subscription identifier is sent and received', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: false),
      ),
    );
    await connectFuture;

    final subscribeFuture = client.subscribe(
      'sub/topic',
      subscriptionIdentifier: 42,
    );
    final subscribe = await _nextPacket(transport) as MqttSubscribePacket;
    expect(
      subscribe.properties,
      contains(const SubscriptionIdentifier(42)),
    );
    transport.inject(
      MqttPacketCodec.encode(
        MqttSubackPacket(
          packetIdentifier: subscribe.packetIdentifier,
          reasonCodes: const [0],
        ),
      ),
    );
    await subscribeFuture;

    await client.disconnect();
  });

  test('message exposes subscription identifiers', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: false),
      ),
    );
    await connectFuture;

    final messageFuture = client.messages.first;
    transport.inject(
      MqttPacketCodec.encode(
        MqttPublishPacket(
          topicName: 't',
          payload: Uint8List.fromList([1]),
          properties: const [
            SubscriptionIdentifier(1),
            SubscriptionIdentifier(2),
          ],
        ),
      ),
    );

    final message = await messageFuture;
    expect(message.subscriptionIdentifiers, [1, 2]);

    await client.disconnect();
  });

  test('shared subscription rejected when unavailable', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          properties: [SharedSubscriptionAvailable(0)],
        ),
      ),
    );
    await connectFuture;

    await expectLater(
      client.subscribe(r'$share/group/topic'),
      throwsA(isA<Exception>()),
    );

    await client.disconnect();
  });
}

Future<MqttPacket> _nextPacket(MemoryTransport transport) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (true) {
    final bytes = transport.takeOutgoingBytes();
    if (bytes.isNotEmpty) {
      return MqttPacketCodec.decode(bytes);
    }
    if (DateTime.now().isAfter(deadline)) {
      fail('Timed out waiting for a packet from the client');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

Future<void> _waitFor(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('Timed out waiting for condition');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
