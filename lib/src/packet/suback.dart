import '../codec/mqtt_reader.dart';
import '../codec/mqtt_writer.dart';
import '../exception/mqtt_exception.dart';
import '../property/mqtt_property.dart';
import '../property/property_codec.dart';
import 'mqtt_packet.dart';
import 'mqtt_reason_code.dart';

/// SUBACK packet (specification section 3.9).
final class MqttSubackPacket extends MqttPacket
    implements MqttPacketWithIdentifier {
  MqttSubackPacket({
    required this.packetIdentifier,
    required this.reasonCodes,
    this.properties = const [],
  }) {
    if (reasonCodes.isEmpty) {
      throw ArgumentError.value(
        reasonCodes,
        'reasonCodes',
        'SUBACK requires at least one reason code',
      );
    }
  }

  @override
  final int packetIdentifier;

  /// One reason code per topic filter. Values 0/1/2 are granted QoS; values
  /// >= 0x80 are failures.
  final List<int> reasonCodes;

  final List<MqttProperty> properties;

  @override
  MqttPacketType get type => MqttPacketType.suback;

  @override
  void encodeBody(MqttWriter writer) {
    writePacketIdentifier(writer, packetIdentifier);
    PropertyCodec.encode(writer, properties, MqttPropertyContext.suback);
    for (final reasonCode in reasonCodes) {
      writer.writeByte(reasonCode);
    }
  }

  static MqttSubackPacket decode(MqttReader reader) {
    final packetIdentifier = readPacketIdentifier(reader);
    final properties = PropertyCodec.decode(
      reader,
      MqttPropertyContext.suback,
    );
    final reasonCodes = <int>[];
    while (reader.hasRemaining) {
      final reasonCode = reader.readByte();
      if (!subackReasonCodes.contains(reasonCode)) {
        throw MqttProtocolException(
          'Invalid SUBACK reason code: $reasonCode',
        );
      }
      reasonCodes.add(reasonCode);
    }
    if (reasonCodes.isEmpty) {
      throw MqttMalformedPacketException(
        'SUBACK must contain at least one reason code',
      );
    }
    return MqttSubackPacket(
      packetIdentifier: packetIdentifier,
      reasonCodes: reasonCodes,
      properties: properties,
    );
  }
}
