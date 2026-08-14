import '../subscription.dart';

/// Tracks the client's active subscriptions so they can be re-established
/// when a session is lost.
final class SubscriptionStore {
  final Map<String, MqttSubscription> _subscriptions = <String, MqttSubscription>{};

  int get count => _subscriptions.length;

  bool contains(String topicFilter) => _subscriptions.containsKey(topicFilter);

  MqttSubscription? operator [](String topicFilter) => _subscriptions[topicFilter];

  List<MqttSubscription> get all => List.unmodifiable(_subscriptions.values);

  void add(MqttSubscription subscription) {
    _subscriptions[subscription.topicFilter] = subscription;
  }

  void remove(String topicFilter) {
    _subscriptions.remove(topicFilter);
  }

  void clear() => _subscriptions.clear();
}
