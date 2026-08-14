import '../codec/mqtt_reader.dart';
import '../property/mqtt_property.dart';
import 'mqtt_packet.dart';
import 'mqtt_pub_reply.dart';
import 'mqtt_reason_code.dart';

/// PUBREC packet (specification section 3.5).
final class MqttPubrecPacket extends MqttPubReplyPacket {
  const MqttPubrecPacket({
    required super.packetIdentifier,
    super.reasonCode,
    super.properties,
  });

  @override
  MqttPacketType get type => MqttPacketType.pubrec;

  @override
  MqttPropertyContext get propertyContext => MqttPropertyContext.pubrec;

  static MqttPubrecPacket decode(MqttReader reader) {
    final fields = decodePubReplyBody(
      reader,
      pubackReasonCodes,
      MqttPropertyContext.pubrec,
    );
    return MqttPubrecPacket(
      packetIdentifier: fields.packetIdentifier,
      reasonCode: fields.reasonCode,
      properties: fields.properties,
    );
  }
}
