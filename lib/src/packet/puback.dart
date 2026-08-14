import '../codec/mqtt_reader.dart';
import '../property/mqtt_property.dart';
import 'mqtt_packet.dart';
import 'mqtt_pub_reply.dart';
import 'mqtt_reason_code.dart';

/// PUBACK packet (specification section 3.4).
final class MqttPubackPacket extends MqttPubReplyPacket {
  const MqttPubackPacket({
    required super.packetIdentifier,
    super.reasonCode,
    super.properties,
  });

  @override
  MqttPacketType get type => MqttPacketType.puback;

  @override
  MqttPropertyContext get propertyContext => MqttPropertyContext.puback;

  static MqttPubackPacket decode(MqttReader reader) {
    final fields = decodePubReplyBody(
      reader,
      pubackReasonCodes,
      MqttPropertyContext.puback,
    );
    return MqttPubackPacket(
      packetIdentifier: fields.packetIdentifier,
      reasonCode: fields.reasonCode,
      properties: fields.properties,
    );
  }
}
