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

  /// Discards all session state.
  void reset() {
    packetIds.reset();
    outgoingQos1.clear();
    outgoingQos2.clear();
    incomingQos2.clear();
    subscriptions.clear();
  }
}
