import 'dart:typed_data';

import 'package:mqtt5/src/client/mqtt_client.dart';
import 'package:mqtt5/src/mqtt_qos.dart';
import 'package:mqtt5/src/packet/connack.dart';
import 'package:mqtt5/src/packet/mqtt_packet.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/packet/mqtt_reason_code.dart';
import 'package:mqtt5/src/packet/puback.dart';
import 'package:mqtt5/src/packet/publish.dart';
import 'package:mqtt5/src/transport/memory_transport.dart';
import 'package:test/test.dart';

void main() {
  test('outgoing QoS1 publish completes on PUBACK', () async {
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

    final publishFuture = client.publish(
      'a/b',
      Uint8List.fromList([1, 2, 3]),
      qos: MqttQos.atLeastOnce,
    );

    final publish = await _nextPacket(transport) as MqttPublishPacket;
    expect(publish.qos, MqttQos.atLeastOnce);
    expect(publish.packetIdentifier, isNot(0));
    expect(publish.topicName, 'a/b');
    expect(publish.payload, [1, 2, 3]);

    transport.inject(
      MqttPacketCodec.encode(
        MqttPubackPacket(packetIdentifier: publish.packetIdentifier),
      ),
    );

    final result = await publishFuture;
    expect(result.reasonCode, MqttReasonCode.success);

    await client.disconnect();
  });

  test('incoming QoS1 publish is delivered and PUBACK is sent', () async {
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
          topicName: 'incoming/qos1',
          payload: Uint8List.fromList([7]),
          qos: MqttQos.atLeastOnce,
          packetIdentifier: 55,
        ),
      ),
    );

    final message = await messageFuture;
    expect(message.topic, 'incoming/qos1');
    expect(message.qos, MqttQos.atLeastOnce);
    expect(message.duplicate, isFalse);

    final ack = await _nextPacket(transport) as MqttPubackPacket;
    expect(ack.packetIdentifier, 55);

    await client.disconnect();
  });

  test('duplicate incoming QoS1 publish sets duplicate flag', () async {
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

    final messages = <dynamic>[];
    final sub = client.messages.listen(messages.add);

    transport.inject(
      MqttPacketCodec.encode(
        MqttPublishPacket(
          topicName: 't',
          payload: Uint8List.fromList([1]),
          qos: MqttQos.atLeastOnce,
          packetIdentifier: 9,
          dup: true,
        ),
      ),
    );

    await _waitFor(() => messages.isNotEmpty);
    expect(messages.single.duplicate, isTrue);
    await _nextPacket(transport); // consume PUBACK

    await sub.cancel();
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
