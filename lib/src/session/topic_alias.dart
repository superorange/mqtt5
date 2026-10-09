import '../exception/mqtt_exception.dart';

/// Maintains Topic Alias mappings for one direction of a network connection.
///
/// Alias mappings are per-connection and must be reset on reconnect.
final class TopicAliasMap {
  TopicAliasMap({
    this.maximum = 0,
    this.enableEviction = false,
  });

  /// The maximum alias value permitted in this direction.
  int maximum;

  /// Whether to evict the least recently used alias when all alias slots
  /// are occupied. Defaults to false.
  bool enableEviction;

  final Map<int, String> _byAlias = <int, String>{};
  final Map<String, int> _byTopic = <String, int>{};
  final List<int> _lruOrder = <int>[];
  int _next = 1;

  /// Resolves an alias to its topic, or null if unknown.
  String? resolve(int alias) => _byAlias[alias];

  /// Returns the alias already assigned to [topic], if any.
  int? aliasFor(String topic) {
    final alias = _byTopic[topic];
    if (alias != null) {
      _touch(alias);
    }
    return alias;
  }

  /// Registers a topic under an alias (incoming direction).
  ///
  /// A sender may rebind an alias to a different topic at any time by sending
  /// the full topic name alongside it (specification section 3.3.2.3.4).
  void register(int alias, String topic) {
    if (alias < 1 || alias > maximum) {
      throw MqttTopicAliasInvalidException(
        'Topic alias $alias exceeds the negotiated maximum $maximum',
      );
    }
    _bind(alias, topic);
  }

  /// Reserves the next free alias without binding it to a topic, or returns
  /// null when the alias space is exhausted (unless [enableEviction] is true,
  /// in which case the least recently used alias is returned).
  ///
  /// The caller must [commit] the alias only once the PUBLISH carrying the
  /// full topic name has actually been written: a mapping the server never
  /// saw would make every later publish reference an unknown alias.
  int? reserve({bool? evict}) {
    if (maximum <= 0) {
      return null;
    }
    for (var i = 0; i < maximum; i++) {
      final candidate = _next;
      _next = _next >= maximum ? 1 : _next + 1;
      if (!_byAlias.containsKey(candidate)) {
        return candidate;
      }
    }
    final shouldEvict = evict ?? enableEviction;
    if (shouldEvict && _lruOrder.isNotEmpty) {
      return _lruOrder.first;
    }
    return null;
  }

  /// Binds a reserved alias to [topic].
  void commit(int alias, String topic) => _bind(alias, topic);

  /// Installs [alias] -> [topic].
  ///
  /// Rebinding an alias must drop the reverse entry the previous topic held,
  /// or [_byTopic] keeps a mapping the peer no longer honours: for the
  /// outgoing direction that would publish to the wrong topic, and for the
  /// incoming direction it is growth a peer can drive without bound by cycling
  /// one alias through many topic names.
  ///
  /// The reverse is deliberately not symmetric. Two aliases are allowed to
  /// name the same topic, so binding a topic that already has an alias must
  /// leave the older alias resolvable; only [_byTopic] moves to the newer one.
  void _bind(int alias, String topic) {
    final previousTopic = _byAlias[alias];
    if (previousTopic != null && previousTopic != topic) {
      _byTopic.remove(previousTopic);
    }
    _byAlias[alias] = topic;
    _byTopic[topic] = alias;
    _touch(alias);
  }

  void _touch(int alias) {
    _lruOrder.remove(alias);
    _lruOrder.add(alias);
  }

  void reset() {
    _byAlias.clear();
    _byTopic.clear();
    _lruOrder.clear();
    _next = 1;
  }
}
