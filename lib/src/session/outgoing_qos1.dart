import 'dart:async';
import 'dart:typed_data';

import '../client/mqtt_publish_result.dart';
import '../property/mqtt_property.dart';

/// A QoS 1 PUBLISH awaiting PUBACK.
final class OutgoingQos1Entry {
  OutgoingQos1Entry({
    required this.packetIdentifier,
    required this.sequence,
    required this.topic,
    required this.payload,
    required this.retain,
    required this.properties,
  });

  final int packetIdentifier;

  /// Publish order within the session, used to re-send in the order the
  /// original PUBLISH packets were sent (MQTT-4.6.0-1).
  final int sequence;

  final String topic;
  final Uint8List payload;
  final bool retain;
  final List<MqttProperty> properties;

  final Completer<MqttPublishResult> completer = Completer<MqttPublishResult>();
}

/// Tracks outgoing QoS 1 messages awaiting PUBACK.
final class OutgoingQos1Store {
  final Map<int, OutgoingQos1Entry> _entries = <int, OutgoingQos1Entry>{};

  int get count => _entries.length;

  Iterable<OutgoingQos1Entry> get entries => _entries.values;

  OutgoingQos1Entry? operator [](int packetIdentifier) =>
      _entries[packetIdentifier];

  void put(OutgoingQos1Entry entry) {
    _entries[entry.packetIdentifier] = entry;
  }

  OutgoingQos1Entry? remove(int packetIdentifier) =>
      _entries.remove(packetIdentifier);

  void clear() => _entries.clear();
}
