import 'dart:typed_data';

import 'package:mqtt5/src/client/mqtt_client.dart';
import 'package:mqtt5/src/mqtt_qos.dart';
import 'package:mqtt5/src/packet/connack.dart';
import 'package:mqtt5/src/packet/mqtt_packet.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/packet/mqtt_reason_code.dart';
import 'package:mqtt5/src/packet/pubcomp.dart';
import 'package:mqtt5/src/packet/publish.dart';
import 'package:mqtt5/src/packet/pubrec.dart';
import 'package:mqtt5/src/packet/pubrel.dart';
import 'package:mqtt5/src/transport/memory_transport.dart';
import 'package:test/test.dart';

void main() {
  test('outgoing QoS2 completes the full exchange', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);
    await _connect(client, transport);

    final publishFuture = client.publish(
      'qos2/topic',
      Uint8List.fromList([1, 2]),
      qos: MqttQos.exactlyOnce,
    );

    final publish = await _nextPacket(transport) as MqttPublishPacket;
    expect(publish.qos, MqttQos.exactlyOnce);
    final id = publish.packetIdentifier;
    expect(id, isNot(0));

    transport.inject(
      MqttPacketCodec.encode(MqttPubrecPacket(packetIdentifier: id)),
    );
    final pubrel = await _nextPacket(transport) as MqttPubrelPacket;
    expect(pubrel.packetIdentifier, id);

    transport.inject(
      MqttPacketCodec.encode(MqttPubcompPacket(packetIdentifier: id)),
    );
    final result = await publishFuture;
    expect(result.reasonCode, MqttReasonCode.success);

    await client.disconnect();
  });

  test('outgoing QoS2 PUBREC error aborts the exchange', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);
    await _connect(client, transport);

    final publishFuture = client.publish(
      'qos2/topic',
      Uint8List.fromList([1]),
      qos: MqttQos.exactlyOnce,
    );
    final publish = await _nextPacket(transport) as MqttPublishPacket;

    transport.inject(
      MqttPacketCodec.encode(
        MqttPubrecPacket(
          packetIdentifier: publish.packetIdentifier,
          reasonCode: MqttReasonCode.notAuthorized,
        ),
      ),
    );

    final result = await publishFuture;
    expect(result.reasonCode, MqttReasonCode.notAuthorized);

    // No PUBREL should follow an error PUBREC.
    final outgoing = transport.takeOutgoingBytes();
    expect(outgoing, isEmpty);

    await client.disconnect();
  });

  test('incoming QoS2 is delivered once and acknowledged', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);
    await _connect(client, transport);

    final messages = <dynamic>[];
    final sub = client.messages.listen(messages.add);

    transport.inject(
      MqttPacketCodec.encode(
        MqttPublishPacket(
          topicName: 'in/qos2',
          payload: Uint8List.fromList([9]),
          qos: MqttQos.exactlyOnce,
          packetIdentifier: 100,
        ),
      ),
    );
    final pubrec = await _nextPacket(transport) as MqttPubrecPacket;
    expect(pubrec.packetIdentifier, 100);

    // Duplicate PUBLISH with DUP flag.
    transport.inject(
      MqttPacketCodec.encode(
        MqttPublishPacket(
          topicName: 'in/qos2',
          payload: Uint8List.fromList([9]),
          qos: MqttQos.exactlyOnce,
          packetIdentifier: 100,
          dup: true,
        ),
      ),
    );
    final pubrec2 = await _nextPacket(transport) as MqttPubrecPacket;
    expect(pubrec2.packetIdentifier, 100);
    expect(messages, hasLength(1));

    transport.inject(
      MqttPacketCodec.encode(MqttPubrelPacket(packetIdentifier: 100)),
    );
    final pubcomp = await _nextPacket(transport) as MqttPubcompPacket;
    expect(pubcomp.packetIdentifier, 100);
    expect(pubcomp.reasonCode, isNull);

    await sub.cancel();
    await client.disconnect();
  });

  test('PUBREL for unknown identifier returns packet identifier not found', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);
    await _connect(client, transport);

    transport.inject(
      MqttPacketCodec.encode(MqttPubrelPacket(packetIdentifier: 777)),
    );
    final pubcomp = await _nextPacket(transport) as MqttPubcompPacket;
    expect(pubcomp.packetIdentifier, 777);
    expect(pubcomp.reasonCode, MqttReasonCode.packetIdentifierNotFound);

    await client.disconnect();
  });
}

Future<void> _connect(MqttClient client, MemoryTransport transport) async {
  final connectFuture = client.connect();
  await _nextPacket(transport);
  transport.inject(
    MqttPacketCodec.encode(
      const MqttConnackPacket(sessionPresent: false),
    ),
  );
  await connectFuture;
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
