import 'dart:typed_data';

import '../codec/mqtt_reader.dart';
import '../codec/mqtt_utf8.dart';
import '../codec/mqtt_writer.dart';
import '../exception/mqtt_exception.dart';
import '../mqtt_qos.dart';
import '../property/mqtt_property.dart';
import '../property/property_codec.dart';
import '../topic.dart';
import 'mqtt_packet.dart';

/// PUBLISH packet (specification section 3.3).
final class MqttPublishPacket extends MqttPacket
    implements MqttPacketWithIdentifier {
  MqttPublishPacket({
    required this.topicName,
    required this.payload,
    this.qos = MqttQos.atMostOnce,
    this.retain = false,
    this.dup = false,
    this.packetIdentifier = 0,
    this.properties = const [],
  }) {
    if (qos != MqttQos.atMostOnce &&
        (packetIdentifier < 1 || packetIdentifier > 0xFFFF)) {
      throw ArgumentError.value(
        packetIdentifier,
        'packetIdentifier',
        'QoS > 0 requires a packet identifier between 1 and 65535',
      );
    }
    if (qos == MqttQos.atMostOnce && dup) {
      throw ArgumentError.value(
        dup,
        'dup',
        'DUP flag must be 0 for QoS 0 messages',
      );
    }
  }

  final String topicName;
  final Uint8List payload;
  final MqttQos qos;
  final bool retain;
  final bool dup;
  @override
  final int packetIdentifier;
  final List<MqttProperty> properties;

  @override
  MqttPacketType get type => MqttPacketType.publish;

  @override
  int get fixedHeaderFlags =>
      (dup ? 0x8 : 0x0) | (qos.value << 1) | (retain ? 0x1 : 0x0);

  @override
  void encodeBody(MqttWriter writer) {
    MqttUtf8.encodeTo(writer, topicName);
    if (qos != MqttQos.atMostOnce) {
      writePacketIdentifier(writer, packetIdentifier);
    }
    PropertyCodec.encode(writer, properties, MqttPropertyContext.publish);
    writer.writeBytes(payload);
  }

  static MqttPublishPacket decode(MqttReader reader, int flags) {
    final dup = flags & 0x08 != 0;
    final qosValue = (flags >> 1) & 0x03;
    if (qosValue == 3) {
      throw MqttMalformedPacketException('PUBLISH QoS must not be 3');
    }
    final retain = flags & 0x01 != 0;
    final qos = MqttQos.fromValue(qosValue);
    if (qos == MqttQos.atMostOnce && dup) {
      throw MqttMalformedPacketException(
        'PUBLISH DUP flag must be 0 for QoS 0 messages',
      );
    }

    final topicName = MqttUtf8.decode(reader);
    if (topicName.isNotEmpty) {
      // MQTT-3.3.2-2: the Topic Name in a PUBLISH must not hold wildcards.
      // An empty Topic Name is legal and means the topic comes from a Topic
      // Alias, so only a non-empty one is a Topic Name to check here.
      final problem = MqttTopic.checkName(topicName);
      if (problem != null) {
        throw MqttProtocolException('PUBLISH topic name $problem');
      }
    }
    int packetIdentifier = 0;
    if (qos != MqttQos.atMostOnce) {
      packetIdentifier = reader.readUint16();
      if (packetIdentifier < 1 || packetIdentifier > 0xFFFF) {
        throw MqttMalformedPacketException(
          'Invalid packet identifier: $packetIdentifier',
        );
      }
    }
    final properties = PropertyCodec.decode(
      reader,
      MqttPropertyContext.publish,
    );
    final payload = reader.readRemaining();

    return MqttPublishPacket(
      topicName: topicName,
      payload: payload,
      qos: qos,
      retain: retain,
      dup: dup,
      packetIdentifier: packetIdentifier,
      properties: properties,
    );
  }
}
