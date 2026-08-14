import 'dart:typed_data';

import '../mqtt_qos.dart';
import '../property/mqtt_property.dart';

/// An application message received from the broker.
final class MqttMessage {
  const MqttMessage({
    required this.topic,
    required this.payload,
    required this.qos,
    this.retain = false,
    this.duplicate = false,
    this.properties = const [],
  });

  final String topic;
  final Uint8List payload;
  final MqttQos qos;
  final bool retain;
  final bool duplicate;
  final List<MqttProperty> properties;
}
