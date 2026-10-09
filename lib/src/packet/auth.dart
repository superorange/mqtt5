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

  /// Section 3.15.2.1: the Reason Code and the Property Length may only be
  /// left out together, when the Reason Code is 0x00 (Success) and there are
  /// no properties. Unlike DISCONNECT (section 3.14.2.2.1) or PUBACK (section
  /// 3.4.2.2.1), AUTH has no form that keeps the Reason Code and drops the
  /// Property Length, so a Remaining Length of 1 is never written.
  @override
  void encodeBody(MqttWriter writer) {
    final code = reasonCode;
    if (properties.isEmpty &&
        (code == null || code == MqttReasonCode.success)) {
      return;
    }
    writer.writeByte(code?.value ?? 0x00);
    PropertyCodec.encode(writer, properties, MqttPropertyContext.auth);
  }

  /// Reads leniently: a Reason Code without a Property Length, which the
  /// encoder never produces, is read as having no properties.
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
