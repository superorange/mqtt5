import '../subscription.dart';

/// A subscription together with the Subscription Identifier it was registered
/// under, so a re-subscribe after a session loss reproduces it exactly.
final class StoredSubscription {
  const StoredSubscription(this.subscription, {this.subscriptionIdentifier});

  final MqttSubscription subscription;

  /// The Subscription Identifier sent with the original SUBSCRIBE, if any.
  final int? subscriptionIdentifier;

  String get topicFilter => subscription.topicFilter;
}

/// Tracks the client's active subscriptions so they can be re-established
/// when a session is lost.
final class SubscriptionStore {
  final Map<String, StoredSubscription> _subscriptions =
      <String, StoredSubscription>{};

  int get count => _subscriptions.length;

  void add(MqttSubscription subscription, {int? subscriptionIdentifier}) {
    _subscriptions[subscription.topicFilter] = StoredSubscription(
      subscription,
      subscriptionIdentifier: subscriptionIdentifier,
    );
  }

  void remove(String topicFilter) {
    _subscriptions.remove(topicFilter);
  }

  void clear() => _subscriptions.clear();

  /// Groups the stored subscriptions by Subscription Identifier.
  ///
  /// Each group becomes one SUBSCRIBE packet on re-subscribe, because a
  /// SUBSCRIBE carries at most one Subscription Identifier for all of its
  /// topic filters.
  Map<int?, List<MqttSubscription>> groupedByIdentifier() {
    final groups = <int?, List<MqttSubscription>>{};
    for (final stored in _subscriptions.values) {
      groups
          .putIfAbsent(stored.subscriptionIdentifier, () => <MqttSubscription>[])
          .add(stored.subscription);
    }
    return groups;
  }
}
