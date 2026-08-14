import '../codec/mqtt_reader.dart';
import '../codec/mqtt_utf8.dart';
import '../codec/mqtt_writer.dart';
import '../exception/mqtt_exception.dart';
import '../property/mqtt_property.dart';
import '../property/property_codec.dart';
import 'mqtt_packet.dart';

/// UNSUBSCRIBE packet (specification section 3.10).
final class MqttUnsubscribePacket extends MqttPacket
    implements MqttPacketWithIdentifier {
  MqttUnsubscribePacket({
    required this.packetIdentifier,
    required this.topicFilters,
    this.properties = const [],
  }) {
    if (topicFilters.isEmpty) {
      throw ArgumentError.value(
        topicFilters,
        'topicFilters',
        'UNSUBSCRIBE requires at least one topic filter',
      );
    }
  }

  @override
  final int packetIdentifier;
  final List<String> topicFilters;
  final List<MqttProperty> properties;

  @override
  MqttPacketType get type => MqttPacketType.unsubscribe;

  @override
  void encodeBody(MqttWriter writer) {
    writePacketIdentifier(writer, packetIdentifier);
    PropertyCodec.encode(writer, properties, MqttPropertyContext.unsubscribe);
    for (final topicFilter in topicFilters) {
      MqttUtf8.encodeTo(writer, topicFilter);
    }
  }

  static MqttUnsubscribePacket decode(MqttReader reader) {
    final packetIdentifier = reader.readUint16();
    final properties = PropertyCodec.decode(
      reader,
      MqttPropertyContext.unsubscribe,
    );
    final topicFilters = <String>[];
    while (reader.hasRemaining) {
      topicFilters.add(MqttUtf8.decode(reader));
    }
    if (topicFilters.isEmpty) {
      throw MqttMalformedPacketException(
        'UNSUBSCRIBE must contain at least one topic filter',
      );
    }
    return MqttUnsubscribePacket(
      packetIdentifier: packetIdentifier,
      topicFilters: topicFilters,
      properties: properties,
    );
  }
}
