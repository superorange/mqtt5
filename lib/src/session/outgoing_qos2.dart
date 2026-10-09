import 'dart:async';
import 'dart:typed_data';

import '../client/mqtt_publish_result.dart';
import '../property/mqtt_property.dart';
import 'awaiting_ack.dart';

/// The state of an outgoing QoS 2 exchange.
enum OutgoingQos2State {
  publishSent,
  pubRelSent,
}

/// A QoS 2 PUBLISH in progress.
final class OutgoingQos2Entry with AwaitingAck {
  OutgoingQos2Entry({
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

  OutgoingQos2State state = OutgoingQos2State.publishSent;

  /// Order in which the PUBREC for this exchange arrived, used to replay
  /// PUBREL packets in that same order (MQTT-4.6.0-4). Zero until PUBREC
  /// arrives, which is also when [state] leaves [OutgoingQos2State.publishSent].
  int pubrecSequence = 0;

  /// Whether the PUBLISH has been put on the wire with DUP set.
  bool retransmitted = false;

  /// Whether the PUBREL has been re-sent on a later connection, so a PUBCOMP
  /// 0x92 ("not found") only means the server already finished the exchange.
  bool pubrelResent = false;

  final Completer<MqttPublishResult> completer;

  /// The same publication under a new packet identifier, for a server that
  /// refused the original one as still in use (reason code 0x91).
  OutgoingQos2Entry withIdentifier(int packetIdentifier) => OutgoingQos2Entry(
        packetIdentifier: packetIdentifier,
        sequence: sequence,
        topic: topic,
        payload: payload,
        retain: retain,
        properties: properties,
        completer: completer,
      );
}

/// Tracks outgoing QoS 2 messages through the PUBLISH/PUBREC/PUBREL/PUBCOMP
/// exchange.
final class OutgoingQos2Store {
  final Map<int, OutgoingQos2Entry> _entries = <int, OutgoingQos2Entry>{};

  int get count => _entries.length;

  Iterable<OutgoingQos2Entry> get entries => _entries.values;

  OutgoingQos2Entry? operator [](int packetIdentifier) =>
      _entries[packetIdentifier];

  void put(OutgoingQos2Entry entry) {
    _entries[entry.packetIdentifier] = entry;
  }

  OutgoingQos2Entry? remove(int packetIdentifier) =>
      _entries.remove(packetIdentifier);

  void clear() => _entries.clear();
}
