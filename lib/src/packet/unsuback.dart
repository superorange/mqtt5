import '../codec/mqtt_reader.dart';
import '../codec/mqtt_writer.dart';
import '../exception/mqtt_exception.dart';
import '../property/mqtt_property.dart';
import '../property/property_codec.dart';
import 'mqtt_packet.dart';
import 'mqtt_reason_code.dart';

/// UNSUBACK packet (specification section 3.11).
final class MqttUnsubackPacket extends MqttPacket
    implements MqttPacketWithIdentifier {
  MqttUnsubackPacket({
    required this.packetIdentifier,
    required this.reasonCodes,
    this.properties = const [],
  }) {
    if (reasonCodes.isEmpty) {
      throw ArgumentError.value(
        reasonCodes,
        'reasonCodes',
        'UNSUBACK requires at least one reason code',
      );
    }
  }

  @override
  final int packetIdentifier;
  final List<int> reasonCodes;
  final List<MqttProperty> properties;

  @override
  MqttPacketType get type => MqttPacketType.unsuback;

  @override
  void encodeBody(MqttWriter writer) {
    writePacketIdentifier(writer, packetIdentifier);
    PropertyCodec.encode(writer, properties, MqttPropertyContext.unsuback);
    for (final reasonCode in reasonCodes) {
      writer.writeByte(reasonCode);
    }
  }

  static MqttUnsubackPacket decode(MqttReader reader) {
    final packetIdentifier = readPacketIdentifier(reader);
    final properties = PropertyCodec.decode(
      reader,
      MqttPropertyContext.unsuback,
    );
    final reasonCodes = <int>[];
    while (reader.hasRemaining) {
      final reasonCode = reader.readByte();
      if (!unsubackReasonCodes.contains(reasonCode)) {
        throw MqttProtocolException(
          'Invalid UNSUBACK reason code: $reasonCode',
        );
      }
      reasonCodes.add(reasonCode);
    }
    if (reasonCodes.isEmpty) {
      throw MqttMalformedPacketException(
        'UNSUBACK must contain at least one reason code',
      );
    }
    return MqttUnsubackPacket(
      packetIdentifier: packetIdentifier,
      reasonCodes: reasonCodes,
      properties: properties,
    );
  }
}
