import '../codec/mqtt_reader.dart';
import '../codec/mqtt_writer.dart';
import '../exception/mqtt_exception.dart';
import '../property/mqtt_property.dart';
import '../property/property_codec.dart';
import 'mqtt_packet.dart';
import 'mqtt_reason_code.dart';

/// CONNACK packet (specification section 3.2).
final class MqttConnackPacket extends MqttPacket {
  const MqttConnackPacket({
    required this.sessionPresent,
    this.reasonCode = MqttReasonCode.success,
    this.properties = const [],
  });

  final bool sessionPresent;
  final MqttReasonCode reasonCode;
  final List<MqttProperty> properties;

  @override
  MqttPacketType get type => MqttPacketType.connack;

  @override
  void encodeBody(MqttWriter writer) {
    writer.writeByte(sessionPresent ? 0x01 : 0x00);
    writer.writeByte(reasonCode.value);
    PropertyCodec.encode(writer, properties, MqttPropertyContext.connack);
  }

  static MqttConnackPacket decode(MqttReader reader) {
    final flags = reader.readByte();
    if (flags & 0xFE != 0) {
      throw MqttMalformedPacketException(
        'CONNACK reserved flags must be 0',
      );
    }
    final sessionPresent = flags & 0x01 != 0;
    final reasonCodeValue = reader.readByte();
    final reasonCode = MqttReasonCode.tryFromValue(reasonCodeValue);
    if (reasonCode == null) {
      throw MqttMalformedPacketException(
        'Unknown CONNACK reason code: $reasonCodeValue',
      );
    }
    if (!connackReasonCodes.contains(reasonCodeValue)) {
      throw MqttProtocolException(
        'Invalid CONNACK reason code: $reasonCodeValue',
      );
    }
    final properties = PropertyCodec.decode(
      reader,
      MqttPropertyContext.connack,
    );
    return MqttConnackPacket(
      sessionPresent: sessionPresent,
      reasonCode: reasonCode,
      properties: properties,
    );
  }
}
