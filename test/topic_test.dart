import 'package:mqtt5/src/topic.dart';
import 'package:test/test.dart';

void main() {
  group('Topic Name (section 4.7)', () {
    for (final topic in [
      'a',
      '/',
      'a/b',
      'a//b',
      'sport/tennis/player1',
      'with space', // MQTT-4.7.3: the space character is allowed
      r'$SYS/broker/uptime',
      'ünïcödé/主题',
    ]) {
      test('accepts "$topic"', () {
        expect(MqttTopic.checkName(topic), isNull);
      });
    }

    test('rejects an empty name (MQTT-4.7.3-1)', () {
      expect(MqttTopic.checkName(''), contains('at least one character'));
    });

    for (final topic in ['#', '+', 'a/#', 'a/+/b', 'a+', 'a#']) {
      test('rejects wildcard "$topic" (MQTT-4.7.0-1)', () {
        expect(MqttTopic.checkName(topic), contains('wildcard'));
      });
    }

    test('rejects the null character (MQTT-4.7.3-2)', () {
      expect(
        MqttTopic.checkName('a${String.fromCharCode(0)}b'),
        contains('null character'),
      );
    });

    test('rejects more than 65535 encoded bytes (MQTT-4.7.3-3)', () {
      expect(MqttTopic.checkName('a' * 65535), isNull);
      expect(MqttTopic.checkName('a' * 65536), contains('65535 bytes'));
      // Three-byte characters hit the limit three times sooner.
      expect(MqttTopic.checkName('主' * 21845), isNull);
      expect(MqttTopic.checkName('主' * 21846), contains('65535 bytes'));
    });
  });

  group('Topic Filter (section 4.7.1)', () {
    for (final filter in [
      '#',
      '+',
      '/',
      'sport',
      'sport/#',
      'sport/tennis/#',
      'sport/+/player1',
      '+/tennis/#',
      '+/+/+',
      'a//b',
      '/#',
    ]) {
      test('accepts "$filter"', () {
        expect(MqttTopic.checkFilter(filter), isNull);
      });
    }

    test('rejects an empty filter (MQTT-4.7.3-1)', () {
      expect(MqttTopic.checkFilter(''), contains('at least one character'));
    });

    for (final filter in ['sport/tennis#', 'a/##', '#abc']) {
      test('rejects "$filter": # must own its level (MQTT-4.7.1-1)', () {
        expect(MqttTopic.checkFilter(filter), contains('entire topic level'));
      });
    }

    for (final filter in ['sport/#/ranking', '#/a']) {
      test('rejects "$filter": # must be last (MQTT-4.7.1-1)', () {
        expect(MqttTopic.checkFilter(filter), contains('last character'));
      });
    }

    for (final filter in ['sport+', 'sport/tennis+', '+abc/x']) {
      test('rejects "$filter": + must own its level (MQTT-4.7.1-2)', () {
        expect(MqttTopic.checkFilter(filter), contains('entire topic level'));
      });
    }
  });

  group('Shared subscription (section 4.8.2)', () {
    for (final filter in [
      r'$share/group/sport/tennis',
      r'$share/g/#',
      r'$share/g/+/x',
      r'$share/g//',
    ]) {
      test('accepts "$filter"', () {
        expect(MqttTopic.checkFilter(filter), isNull);
      });
    }

    test('a filter that merely starts with \$share is not shared', () {
      expect(MqttTopic.isShared(r'$shareholders/x'), isFalse);
      expect(MqttTopic.checkFilter(r'$shareholders/x'), isNull);
    });

    test('rejects a missing ShareName separator (MQTT-4.8.2-1)', () {
      expect(MqttTopic.checkFilter(r'$share/group'), contains('ShareName'));
    });

    test('rejects an empty ShareName (MQTT-4.8.2-1)', () {
      expect(
        MqttTopic.checkFilter(r'$share//topic'),
        contains('at least one character'),
      );
    });

    for (final filter in [r'$share/a+b/x', r'$share/a#b/x']) {
      test('rejects wildcards in the ShareName "$filter" (MQTT-4.8.2-2)', () {
        expect(MqttTopic.checkFilter(filter), contains('must not contain'));
      });
    }

    test('rejects an empty filter after the ShareName (MQTT-4.8.2-2)', () {
      expect(
        MqttTopic.checkFilter(r'$share/group/'),
        contains('followed by a topic filter'),
      );
    });

    test('validates the filter part with the normal rules', () {
      expect(
        MqttTopic.checkFilter(r'$share/group/sport/#/rank'),
        contains('last character'),
      );
    });

    test('sharedFilterOf strips the prefix and ShareName', () {
      expect(MqttTopic.sharedFilterOf(r'$share/group/a/b'), 'a/b');
      expect(MqttTopic.sharedFilterOf(r'$share/g/#'), '#');
      expect(MqttTopic.sharedFilterOf('a/b'), 'a/b');
    });
  });
}
