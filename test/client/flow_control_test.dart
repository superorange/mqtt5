import 'dart:typed_data';

import 'package:mqtt5/src/client/mqtt_client.dart';
import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:mqtt5/src/mqtt_qos.dart';
import 'package:mqtt5/src/packet/connack.dart';
import 'package:mqtt5/src/packet/connect.dart';
import 'package:mqtt5/src/packet/mqtt_packet.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/packet/puback.dart';
import 'package:mqtt5/src/packet/publish.dart';
import 'package:mqtt5/src/property/mqtt_property.dart';
import 'package:mqtt5/src/transport/memory_transport.dart';
import 'package:test/test.dart';

void main() {
  test('Receive Maximum = 1 queues the second QoS1 publish', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          properties: [ReceiveMaximum(1)],
        ),
      ),
    );
    await connectFuture;

    final futureA = client.publish(
      't/a',
      Uint8List.fromList([1]),
      qos: MqttQos.atLeastOnce,
    );
    final a = await _nextPacket(transport) as MqttPublishPacket;
    expect(a.topicName, 't/a');

    // Second publish must wait for the first to be acknowledged.
    final futureB = client.publish(
      't/b',
      Uint8List.fromList([2]),
      qos: MqttQos.atLeastOnce,
    );
    await _delay(const Duration(milliseconds: 50));
    expect(transport.takeOutgoingBytes(), isEmpty);

    transport.inject(
      MqttPacketCodec.encode(MqttPubackPacket(packetIdentifier: a.packetIdentifier)),
    );
    await futureA;

    final b = await _nextPacket(transport) as MqttPublishPacket;
    expect(b.topicName, 't/b');
    transport.inject(
      MqttPacketCodec.encode(MqttPubackPacket(packetIdentifier: b.packetIdentifier)),
    );
    await futureB;

    await client.disconnect();
  });

  test('QoS0 is not limited by Receive Maximum', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          properties: [ReceiveMaximum(1)],
        ),
      ),
    );
    await connectFuture;

    await client.publish('q0', Uint8List.fromList([1]), qos: MqttQos.atMostOnce);
    await client.publish('q0', Uint8List.fromList([2]), qos: MqttQos.atMostOnce);

    // Both QoS0 publishes are sent immediately.
    final chunks = transport.takeOutgoing();
    expect(chunks, hasLength(2));
    final packets = chunks.map(MqttPacketCodec.decode).toList();
    expect(packets.every((p) => p is MqttPublishPacket), isTrue);

    await client.disconnect();
  });

  test('Maximum QoS = 1 rejects QoS2 publish', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          properties: [MaximumQos(1)],
        ),
      ),
    );
    await connectFuture;

    await expectLater(
      client.publish('t', Uint8List.fromList([1]), qos: MqttQos.exactlyOnce),
      throwsA(isA<MqttFlowControlException>()),
    );

    await client.disconnect();
  });

  test('Retain Available = 0 rejects retained publish', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          properties: [RetainAvailable(0)],
        ),
      ),
    );
    await connectFuture;

    await expectLater(
      client.publish('t', Uint8List.fromList([1]), retain: true),
      throwsA(isA<MqttFlowControlException>()),
    );

    await client.disconnect();
  });

  test('server Maximum Packet Size blocks oversized publish', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          properties: [MaximumPacketSize(20)],
        ),
      ),
    );
    await connectFuture;

    await expectLater(
      client.publish(
        't',
        Uint8List.fromList(List.filled(100, 0x42)),
        qos: MqttQos.atMostOnce,
      ),
      throwsA(isA<MqttPacketTooLargeException>()),
    );

    await client.disconnect();
  });

  test('CONNECT declares client Receive Maximum and Maximum Packet Size', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect(
      receiveMaximum: 50,
      maximumPacketSize: 1000,
    );
    final connect = await _nextPacket(transport) as MqttConnectPacket;
    expect(connect.properties, contains(const ReceiveMaximum(50)));
    expect(connect.properties, contains(const MaximumPacketSize(1000)));

    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: false),
      ),
    );
    await connectFuture;

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

Future<void> _delay(Duration duration) => Future<void>.delayed(duration);
