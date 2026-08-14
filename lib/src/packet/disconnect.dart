import '../codec/mqtt_reader.dart';
import '../codec/mqtt_writer.dart';
import '../exception/mqtt_exception.dart';
import '../property/mqtt_property.dart';
import '../property/property_codec.dart';
import 'mqtt_packet.dart';
import 'mqtt_reason_code.dart';

/// DISCONNECT packet (specification section 3.14).
final class MqttDisconnectPacket extends MqttPacket {
  const MqttDisconnectPacket({
    this.reasonCode,
    this.properties = const [],
  });

  final MqttReasonCode? reasonCode;
  final List<MqttProperty> properties;

  @override
  MqttPacketType get type => MqttPacketType.disconnect;

  @override
  void encodeBody(MqttWriter writer) {
    if (reasonCode != null || properties.isNotEmpty) {
      writer.writeByte(reasonCode?.value ?? 0x00);
    }
    if (properties.isNotEmpty) {
      PropertyCodec.encode(writer, properties, MqttPropertyContext.disconnect);
    }
  }

  static MqttDisconnectPacket decode(MqttReader reader) {
    MqttReasonCode? reasonCode;
    List<MqttProperty> properties = const [];
    if (reader.hasRemaining) {
      final reasonCodeValue = reader.readByte();
      if (!disconnectReasonCodes.contains(reasonCodeValue)) {
        throw MqttProtocolException(
          'Invalid DISCONNECT reason code: $reasonCodeValue',
        );
      }
      reasonCode = MqttReasonCode.tryFromValue(reasonCodeValue);
    }
    if (reader.hasRemaining) {
      properties = PropertyCodec.decode(
        reader,
        MqttPropertyContext.disconnect,
      );
    }
    return MqttDisconnectPacket(
      reasonCode: reasonCode,
      properties: properties,
    );
  }
}
