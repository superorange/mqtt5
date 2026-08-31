import '../codec/mqtt_reader.dart';
import '../codec/mqtt_utf8.dart';
import '../codec/mqtt_writer.dart';
import '../exception/mqtt_exception.dart';
import '../property/mqtt_property.dart';
import '../property/property_codec.dart';
import '../subscription.dart';
import 'mqtt_packet.dart';

/// SUBSCRIBE packet (specification section 3.8).
final class MqttSubscribePacket extends MqttPacket
    implements MqttPacketWithIdentifier {
  MqttSubscribePacket({
    required this.packetIdentifier,
    required this.subscriptions,
    this.properties = const [],
  }) {
    if (subscriptions.isEmpty) {
      throw ArgumentError.value(
        subscriptions,
        'subscriptions',
        'SUBSCRIBE requires at least one topic filter',
      );
    }
  }

  @override
  final int packetIdentifier;
  final List<MqttSubscription> subscriptions;
  final List<MqttProperty> properties;

  @override
  MqttPacketType get type => MqttPacketType.subscribe;

  @override
  void encodeBody(MqttWriter writer) {
    writePacketIdentifier(writer, packetIdentifier);
    PropertyCodec.encode(writer, properties, MqttPropertyContext.subscribe);
    for (final subscription in subscriptions) {
      MqttUtf8.encodeTo(writer, subscription.topicFilter);
      writer.writeByte(subscription.options.toByte());
    }
  }

  static MqttSubscribePacket decode(MqttReader reader) {
    final packetIdentifier = readPacketIdentifier(reader);
    final properties = PropertyCodec.decode(
      reader,
      MqttPropertyContext.subscribe,
    );
    final subscriptions = <MqttSubscription>[];
    while (reader.hasRemaining) {
      final topicFilter = MqttUtf8.decode(reader);
      final optionsByte = reader.readByte();
      if (optionsByte & 0xC0 != 0) {
        throw MqttMalformedPacketException(
          'Subscription options reserved bits must be 0',
        );
      }
      subscriptions.add(
        MqttSubscription(
          topicFilter,
          options: MqttSubscriptionOptions.fromByte(optionsByte),
        ),
      );
    }
    if (subscriptions.isEmpty) {
      throw MqttMalformedPacketException(
        'SUBSCRIBE must contain at least one topic filter',
      );
    }
    return MqttSubscribePacket(
      packetIdentifier: packetIdentifier,
      subscriptions: subscriptions,
      properties: properties,
    );
  }
}
