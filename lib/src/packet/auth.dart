import '../codec/mqtt_reader.dart';
import '../codec/mqtt_writer.dart';
import '../exception/mqtt_exception.dart';
import '../property/mqtt_property.dart';
import '../property/property_codec.dart';
import 'mqtt_packet.dart';
import 'mqtt_reason_code.dart';

/// AUTH packet (specification section 3.15).
final class MqttAuthPacket extends MqttPacket {
  const MqttAuthPacket({
    this.reasonCode,
    this.properties = const [],
  });

  final MqttReasonCode? reasonCode;
  final List<MqttProperty> properties;

  @override
  MqttPacketType get type => MqttPacketType.auth;

  @override
  void encodeBody(MqttWriter writer) {
    if (reasonCode != null || properties.isNotEmpty) {
      writer.writeByte(reasonCode?.value ?? 0x00);
    }
    if (properties.isNotEmpty) {
      PropertyCodec.encode(writer, properties, MqttPropertyContext.auth);
    }
  }

  static MqttAuthPacket decode(MqttReader reader) {
    MqttReasonCode? reasonCode;
    List<MqttProperty> properties = const [];
    if (reader.hasRemaining) {
      final reasonCodeValue = reader.readByte();
      if (!authReasonCodes.contains(reasonCodeValue)) {
        throw MqttProtocolException(
          'Invalid AUTH reason code: $reasonCodeValue',
        );
      }
      reasonCode = MqttReasonCode.tryFromValue(reasonCodeValue);
    }
    if (reader.hasRemaining) {
      properties = PropertyCodec.decode(reader, MqttPropertyContext.auth);
    }
    return MqttAuthPacket(
      reasonCode: reasonCode,
      properties: properties,
    );
  }
}
