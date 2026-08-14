import '../codec/mqtt_reader.dart';
import '../codec/mqtt_writer.dart';
import '../exception/mqtt_exception.dart';
import '../property/mqtt_property.dart';
import '../property/property_codec.dart';
import 'mqtt_packet.dart';
import 'mqtt_reason_code.dart';

/// Base class for PUBACK, PUBREC, PUBREL and PUBCOMP.
abstract class MqttPubReplyPacket extends MqttPacket
    implements MqttPacketWithIdentifier {
  const MqttPubReplyPacket({
    required this.packetIdentifier,
    this.reasonCode,
    this.properties = const [],
  });

  @override
  final int packetIdentifier;
  final MqttReasonCode? reasonCode;
  final List<MqttProperty> properties;

  MqttPropertyContext get propertyContext;

  @override
  void encodeBody(MqttWriter writer) {
    writePacketIdentifier(writer, packetIdentifier);
    if (reasonCode != null || properties.isNotEmpty) {
      writer.writeByte(reasonCode?.value ?? 0x00);
    }
    if (properties.isNotEmpty) {
      PropertyCodec.encode(writer, properties, propertyContext);
    }
  }
}

/// The decoded variable header fields of a pub reply packet.
typedef PubReplyFields = ({
  int packetIdentifier,
  MqttReasonCode? reasonCode,
  List<MqttProperty> properties,
});

PubReplyFields decodePubReplyBody(
  MqttReader reader,
  Set<int> allowedCodes,
  MqttPropertyContext context,
) {
  final packetIdentifier = reader.readUint16();
  MqttReasonCode? reasonCode;
  List<MqttProperty> properties = const [];
  if (reader.hasRemaining) {
    final reasonCodeValue = reader.readByte();
    if (!allowedCodes.contains(reasonCodeValue)) {
      throw MqttProtocolException(
        'Invalid reason code $reasonCodeValue for $context',
      );
    }
    reasonCode = MqttReasonCode.tryFromValue(reasonCodeValue);
  }
  if (reader.hasRemaining) {
    properties = PropertyCodec.decode(reader, context);
  }
  return (
    packetIdentifier: packetIdentifier,
    reasonCode: reasonCode,
    properties: properties,
  );
}
