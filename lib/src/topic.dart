import 'dart:convert';

/// Topic Name and Topic Filter rules (specification section 4.7 and 4.8.2).
///
/// The checks return a description of the first violation instead of throwing,
/// because the same rule surfaces as two different failures: a caller passing a
/// bad topic to [MqttClient.publish] gets an [ArgumentError], while a peer
/// sending one on the wire is a protocol error.
abstract final class MqttTopic {
  /// The maximum length of a Topic Name or Filter (MQTT-4.7.3-3).
  static const int maxBytes = 0xFFFF;

  static const String sharedPrefix = r'$share/';

  /// Checks [topic] as a Topic Name — used for PUBLISH, Will Topic and
  /// Response Topic. Returns null when it is valid.
  static String? checkName(String topic) {
    final common = _checkCommon(topic);
    if (common != null) {
      return common;
    }
    // MQTT-4.7.0-1: wildcards may be used in Topic Filters but MUST NOT be
    // used within a Topic Name.
    if (topic.contains('+') || topic.contains('#')) {
      return 'must not contain the wildcard characters "+" or "#"';
    }
    return null;
  }

  /// Checks [filter] as a Topic Filter — used for SUBSCRIBE and UNSUBSCRIBE,
  /// including the `\$share/{ShareName}/{filter}` form. Returns null when it
  /// is valid.
  static String? checkFilter(String filter) {
    final common = _checkCommon(filter);
    if (common != null) {
      return common;
    }
    if (filter.startsWith(sharedPrefix)) {
      return _checkShared(filter);
    }
    return _checkFilterBody(filter);
  }

  /// Whether [filter] is a Shared Subscription filter.
  static bool isShared(String filter) => filter.startsWith(sharedPrefix);

  /// The `{filter}` part of a Shared Subscription, or [filter] itself when it
  /// is not shared. Only meaningful for a filter that passed [checkFilter].
  static String sharedFilterOf(String filter) {
    if (!isShared(filter)) {
      return filter;
    }
    final separator = filter.indexOf('/', sharedPrefix.length);
    if (separator < 0) {
      return filter;
    }
    return filter.substring(separator + 1);
  }

  /// Whether [topic] matches [filter] (specification section 4.7).
  ///
  /// [filter] may be a Shared Subscription; the `\$share/{ShareName}/` prefix
  /// is stripped before matching, because it names the group rather than the
  /// topics. Both arguments are assumed to have passed [checkFilter] and
  /// [checkName] — this answers "does it match", not "is it well formed".
  static bool matches(String filter, String topic) {
    final levels = sharedFilterOf(filter).split('/');
    final topicLevels = topic.split('/');
    // MQTT-4.7.2-1: a filter starting with a wildcard does not match a topic
    // beginning with '$', which keeps "#" from sweeping up $SYS and friends.
    // A filter that names the level literally still matches it.
    if (topicLevels.first.startsWith(r'$') &&
        (levels.first == '#' || levels.first == '+')) {
      return false;
    }
    for (var i = 0; i < levels.length; i++) {
      // "#" matches the parent level too, so "sport/#" matches "sport".
      if (levels[i] == '#') {
        return i == levels.length - 1;
      }
      if (i >= topicLevels.length) {
        return false;
      }
      if (levels[i] != '+' && levels[i] != topicLevels[i]) {
        return false;
      }
    }
    return levels.length == topicLevels.length;
  }

  /// MQTT-4.8.2-1 and MQTT-4.8.2-2.
  static String? _checkShared(String filter) {
    final separator = filter.indexOf('/', sharedPrefix.length);
    if (separator < 0) {
      return 'a shared subscription must be '
          r'"$share/{ShareName}/{filter}"';
    }
    final shareName = filter.substring(sharedPrefix.length, separator);
    if (shareName.isEmpty) {
      return 'the ShareName of a shared subscription must be at least one '
          'character long';
    }
    if (shareName.contains('+') || shareName.contains('#')) {
      return 'the ShareName of a shared subscription must not contain '
          '"+" or "#"';
    }
    final body = filter.substring(separator + 1);
    if (body.isEmpty) {
      return 'the ShareName of a shared subscription must be followed by a '
          'topic filter';
    }
    return _checkFilterBody(body);
  }

  /// MQTT-4.7.1-1 and MQTT-4.7.1-2, applied level by level.
  static String? _checkFilterBody(String filter) {
    final levels = filter.split('/');
    for (var i = 0; i < levels.length; i++) {
      final level = levels[i];
      if (level.contains('#')) {
        // MQTT-4.7.1-1: "#" must be specified on its own or following a topic
        // level separator, and must be the last character of the filter.
        if (level != '#') {
          return r'"#" must occupy an entire topic level';
        }
        if (i != levels.length - 1) {
          return r'"#" must be the last character of a topic filter';
        }
      }
      // MQTT-4.7.1-2: where "+" is used it must occupy an entire level.
      if (level.contains('+') && level != '+') {
        return r'"+" must occupy an entire topic level';
      }
    }
    return null;
  }

  /// The rules shared by Topic Names and Topic Filters (section 4.7.3).
  static String? _checkCommon(String topic) {
    // MQTT-4.7.3-1.
    if (topic.isEmpty) {
      return 'must be at least one character long';
    }
    // MQTT-4.7.3-2. MqttUtf8 rejects this on the wire as well; checking here
    // turns it into an error that names the offending argument. Note that the
    // space character is explicitly allowed (section 4.7.3).
    if (topic.codeUnits.contains(0)) {
      return 'must not contain the null character U+0000';
    }
    // MQTT-4.7.3-3. A UTF-16 code unit encodes to at most three UTF-8 bytes,
    // so a string shorter than a third of the limit cannot exceed it and does
    // not need to be encoded just to be measured.
    if (topic.length > maxBytes ~/ 3 && utf8.encode(topic).length > maxBytes) {
      return 'must not encode to more than $maxBytes bytes';
    }
    return null;
  }
}
