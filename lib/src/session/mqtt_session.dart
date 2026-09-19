import 'incoming_qos2.dart';
import 'outgoing_qos1.dart';
import 'outgoing_qos2.dart';
import 'packet_identifier_pool.dart';
import 'subscription_store.dart';

/// All MQTT session state that must survive a reconnect when the broker
/// resumes the session.
final class MqttSession {
  final PacketIdentifierPool packetIds = PacketIdentifierPool();
  final OutgoingQos1Store outgoingQos1 = OutgoingQos1Store();
  final OutgoingQos2Store outgoingQos2 = OutgoingQos2Store();
  final IncomingQos2Store incomingQos2 = IncomingQos2Store();
  final SubscriptionStore subscriptions = SubscriptionStore();

  int get inflightCount => outgoingQos1.count + outgoingQos2.count;

  int _sequence = 0;

  /// The next publish order number.
  ///
  /// QoS 1 and QoS 2 publications live in separate stores, so re-sending them
  /// store by store would not reproduce the order the application published
  /// them in. MQTT-4.6.0-1 requires that order, so entries carry a sequence
  /// and a resumed session re-sends them sorted by it.
  int nextSequence() => ++_sequence;
}
