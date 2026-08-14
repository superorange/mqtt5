import '../codec/mqtt_reader.dart';
import '../property/mqtt_property.dart';
import 'mqtt_packet.dart';
import 'mqtt_pub_reply.dart';
import 'mqtt_reason_code.dart';

/// PUBCOMP packet (specification section 3.7).
final class MqttPubcompPacket extends MqttPubReplyPacket {
  const MqttPubcompPacket({
    required super.packetIdentifier,
    super.reasonCode,
    super.properties,
  });

  @override
  MqttPacketType get type => MqttPacketType.pubcomp;

  @override
  MqttPropertyContext get propertyContext => MqttPropertyContext.pubcomp;

  static MqttPubcompPacket decode(MqttReader reader) {
    final fields = decodePubReplyBody(
      reader,
      pubrelReasonCodes,
      MqttPropertyContext.pubcomp,
    );
    return MqttPubcompPacket(
      packetIdentifier: fields.packetIdentifier,
      reasonCode: fields.reasonCode,
      properties: fields.properties,
    );
  }
}
