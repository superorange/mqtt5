import '../codec/mqtt_reader.dart';
import '../property/mqtt_property.dart';
import 'mqtt_packet.dart';
import 'mqtt_pub_reply.dart';
import 'mqtt_reason_code.dart';

/// PUBREL packet (specification section 3.6).
final class MqttPubrelPacket extends MqttPubReplyPacket {
  const MqttPubrelPacket({
    required super.packetIdentifier,
    super.reasonCode,
    super.properties,
  });

  @override
  MqttPacketType get type => MqttPacketType.pubrel;

  @override
  MqttPropertyContext get propertyContext => MqttPropertyContext.pubrel;

  static MqttPubrelPacket decode(MqttReader reader) {
    final fields = decodePubReplyBody(
      reader,
      pubrelReasonCodes,
      MqttPropertyContext.pubrel,
    );
    return MqttPubrelPacket(
      packetIdentifier: fields.packetIdentifier,
      reasonCode: fields.reasonCode,
      properties: fields.properties,
    );
  }
}
