import 'dart:async';
import 'dart:typed_data';

import '../client/mqtt_publish_result.dart';
import '../property/mqtt_property.dart';
import 'awaiting_ack.dart';

/// A QoS 1 PUBLISH awaiting PUBACK.
final class OutgoingQos1Entry with AwaitingAck {
  OutgoingQos1Entry({
    required this.packetIdentifier,
    required this.sequence,
    required this.topic,
    required this.payload,
    required this.retain,
    required this.properties,
    Completer<MqttPublishResult>? completer,
  }) : completer = completer ?? Completer<MqttPublishResult>();

  final int packetIdentifier;

  /// Publish order within the session, used to re-send in the order the
  /// original PUBLISH packets were sent (MQTT-4.6.0-1).
  final int sequence;

  final String topic;
  final Uint8List payload;
  final bool retain;
  final List<MqttProperty> properties;

  final Completer<MqttPublishResult> completer;

  /// Whether this publication has been put on the wire with DUP set, i.e.
  /// the server may already hold it.
  bool retransmitted = false;

  /// The same publication under a new packet identifier, for a server that
  /// refused the original one as still in use (reason code 0x91).
  OutgoingQos1Entry withIdentifier(int packetIdentifier) => OutgoingQos1Entry(
        packetIdentifier: packetIdentifier,
        sequence: sequence,
        topic: topic,
        payload: payload,
        retain: retain,
        properties: properties,
        completer: completer,
      );
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
