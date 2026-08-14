import '../exception/mqtt_exception.dart';

/// Maintains Topic Alias mappings for one direction of a network connection.
///
/// Alias mappings are per-connection and must be reset on reconnect.
final class TopicAliasMap {
  TopicAliasMap({this.maximum = 0});

  /// The maximum alias value permitted in this direction.
  int maximum;

  final Map<int, String> _byAlias = <int, String>{};
  final Map<String, int> _byTopic = <String, int>{};
  int _next = 1;

  int get count => _byAlias.length;

  /// Resolves an alias to its topic, or null if unknown.
  String? resolve(int alias) => _byAlias[alias];

  /// Returns the alias already assigned to [topic], if any.
  int? aliasFor(String topic) => _byTopic[topic];

  /// Registers a topic under an alias (incoming direction).
  void register(int alias, String topic) {
    if (alias < 1 || alias > maximum) {
      throw MqttProtocolException(
        'Topic alias $alias exceeds the negotiated maximum $maximum',
      );
    }
    _byAlias[alias] = topic;
    _byTopic[topic] = alias;
  }

  /// Assigns an alias to [topic] (outgoing direction), or returns null when
  /// the alias space is exhausted.
  int? assign(String topic) {
    final existing = _byTopic[topic];
    if (existing != null) {
      return existing;
    }
    if (maximum <= 0) {
      return null;
    }
    for (var i = 0; i < maximum; i++) {
      final candidate = _next;
      _next = _next >= maximum ? 1 : _next + 1;
      if (!_byAlias.containsKey(candidate)) {
        _byAlias[candidate] = topic;
        _byTopic[topic] = candidate;
        return candidate;
      }
    }
    return null;
  }

  void reset() {
    _byAlias.clear();
    _byTopic.clear();
    _next = 1;
  }
}
