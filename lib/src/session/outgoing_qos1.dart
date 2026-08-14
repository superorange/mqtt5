import 'dart:async';
import 'dart:typed_data';

import '../client/mqtt_publish_result.dart';
import '../property/mqtt_property.dart';

/// A QoS 1 PUBLISH awaiting PUBACK.
final class OutgoingQos1Entry {
  OutgoingQos1Entry({
    required this.packetIdentifier,
    required this.topic,
    required this.payload,
    required this.retain,
    required this.properties,
  });

  final int packetIdentifier;
  final String topic;
  final Uint8List payload;
  final bool retain;
  final List<MqttProperty> properties;

  /// Set once the packet has been (re)transmitted with DUP=1.
  bool duplicate = false;

  final Completer<MqttPublishResult> completer = Completer<MqttPublishResult>();
}

/// Tracks outgoing QoS 1 messages awaiting PUBACK.
final class OutgoingQos1Store {
  final Map<int, OutgoingQos1Entry> _entries = <int, OutgoingQos1Entry>{};

  int get count => _entries.length;

  Iterable<OutgoingQos1Entry> get entries => _entries.values;

  bool contains(int packetIdentifier) => _entries.containsKey(packetIdentifier);

  OutgoingQos1Entry? operator [](int packetIdentifier) =>
      _entries[packetIdentifier];

  void put(OutgoingQos1Entry entry) {
    _entries[entry.packetIdentifier] = entry;
  }

  OutgoingQos1Entry? remove(int packetIdentifier) =>
      _entries.remove(packetIdentifier);

  void clear() => _entries.clear();
}
