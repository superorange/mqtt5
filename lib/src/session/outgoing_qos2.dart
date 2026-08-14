import 'dart:async';
import 'dart:typed_data';

import '../client/mqtt_publish_result.dart';
import '../property/mqtt_property.dart';

/// The state of an outgoing QoS 2 exchange.
enum OutgoingQos2State {
  publishSent,
  pubRecReceived,
  pubRelSent,
}

/// A QoS 2 PUBLISH in progress.
final class OutgoingQos2Entry {
  OutgoingQos2Entry({
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

  OutgoingQos2State state = OutgoingQos2State.publishSent;

  /// Set once the PUBLISH has been retransmitted with DUP=1.
  bool duplicate = false;

  final Completer<MqttPublishResult> completer = Completer<MqttPublishResult>();
}

/// Tracks outgoing QoS 2 messages through the PUBLISH/PUBREC/PUBREL/PUBCOMP
/// exchange.
final class OutgoingQos2Store {
  final Map<int, OutgoingQos2Entry> _entries = <int, OutgoingQos2Entry>{};

  int get count => _entries.length;

  Iterable<OutgoingQos2Entry> get entries => _entries.values;

  bool contains(int packetIdentifier) => _entries.containsKey(packetIdentifier);

  OutgoingQos2Entry? operator [](int packetIdentifier) =>
      _entries[packetIdentifier];

  void put(OutgoingQos2Entry entry) {
    _entries[entry.packetIdentifier] = entry;
  }

  OutgoingQos2Entry? remove(int packetIdentifier) =>
      _entries.remove(packetIdentifier);

  void clear() => _entries.clear();
}
