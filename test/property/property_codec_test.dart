import 'dart:typed_data';

import 'package:mqtt5/src/codec/mqtt_reader.dart';
import 'package:mqtt5/src/codec/mqtt_writer.dart';
import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:mqtt5/src/property/mqtt_property.dart';
import 'package:mqtt5/src/property/property_codec.dart';
import 'package:test/test.dart';

void main() {
  group('PropertyCodec round trips', () {
    test('all byte properties', () {
      final properties = [
        const PayloadFormatIndicator(1),
        const RequestProblemInformation(0),
        const RequestResponseInformation(1),
        const RetainAvailable(0),
        const WildcardSubscriptionAvailable(1),
        const SubscriptionIdentifierAvailable(0),
        const SharedSubscriptionAvailable(1),
        const MaximumQos(1),
      ];
      for (final p in properties) {
        _expectRoundTrip(p, MqttPropertyContext.connack);
      }
    });

    test('two byte integer properties', () {
      for (final p in [
        const ServerKeepAlive(120),
        const ReceiveMaximum(10),
        const TopicAliasMaximum(20),
        const TopicAlias(5),
      ]) {
        _expectRoundTrip(p, MqttPropertyContext.connack);
      }
    });

    test('four byte integer properties', () {
      for (final p in [
        const MessageExpiryInterval(3600),
        const SessionExpiryInterval(7200),
        const WillDelayInterval(30),
        const MaximumPacketSize(1024),
      ]) {
        _expectRoundTrip(p, MqttPropertyContext.connack);
      }
    });

    test('utf8 string properties', () {
      for (final p in [
        const ContentType('application/json'),
        const ResponseTopic('reply/here'),
        const AssignedClientIdentifier('server-assigned'),
        const AuthenticationMethod('SCRAM-SHA-256'),
        const ResponseInformation('info'),
        const ServerReference('other.host:1883'),
        const ReasonString('everything is fine'),
      ]) {
        _expectRoundTrip(p, MqttPropertyContext.connack);
      }
    });

    test('binary data properties', () {
      for (final p in [
        CorrelationData([0x01, 0x02, 0x03]),
        AuthenticationData([0xAA, 0xBB]),
      ]) {
        _expectRoundTrip(p, MqttPropertyContext.connack);
      }
    });

    test('variable byte integer property', () {
      _expectRoundTrip(
        const SubscriptionIdentifier(268435455),
        MqttPropertyContext.publish,
      );
    });

    test('user property pair', () {
      const p = UserProperty('traceId', 'abc123');
      _expectRoundTrip(p, MqttPropertyContext.publish);
    });

    test('multiple user properties and subscription identifiers', () {
      final writer = MqttWriter();
      PropertyCodec.encode(writer, const [
        UserProperty('a', '1'),
        UserProperty('b', '2'),
        SubscriptionIdentifier(1),
        SubscriptionIdentifier(2),
        SubscriptionIdentifier(3),
      ], MqttPropertyContext.publish);
      final decoded = PropertyCodec.decode(
        MqttReader(writer.toBytes()),
        MqttPropertyContext.publish,
      );
      expect(decoded, const [
        UserProperty('a', '1'),
        UserProperty('b', '2'),
        SubscriptionIdentifier(1),
        SubscriptionIdentifier(2),
        SubscriptionIdentifier(3),
      ]);
    });
  });

  group('PropertyCodec validation', () {
    test('rejects duplicate non-repeatable property', () {
      final writer = MqttWriter();
      expect(
        () => PropertyCodec.encode(writer, const [
          ReceiveMaximum(10),
          ReceiveMaximum(20),
        ], MqttPropertyContext.connack),
        throwsA(isA<MqttProtocolException>()),
      );
    });

    test('rejects property not allowed in context', () {
      final writer = MqttWriter();
      expect(
        () => PropertyCodec.encode(writer, const [
          ReceiveMaximum(10),
        ], MqttPropertyContext.publish),
        throwsA(isA<MqttProtocolException>()),
      );
    });

    test('rejects ReceiveMaximum of 0', () {
      final writer = MqttWriter();
      expect(
        () => PropertyCodec.encode(writer, const [
          ReceiveMaximum(0),
        ], MqttPropertyContext.connack),
        throwsA(isA<MqttProtocolException>()),
      );
    });

    test('rejects MaximumQos of 2', () {
      final writer = MqttWriter();
      expect(
        () => PropertyCodec.encode(writer, const [
          MaximumQos(2),
        ], MqttPropertyContext.connack),
        throwsA(isA<MqttProtocolException>()),
      );
    });

    test('rejects SubscriptionIdentifier of 0', () {
      final writer = MqttWriter();
      expect(
        () => PropertyCodec.encode(writer, const [
          SubscriptionIdentifier(0),
        ], MqttPropertyContext.publish),
        throwsA(isA<MqttProtocolException>()),
      );
    });

    test('rejects TopicAlias of 0', () {
      final writer = MqttWriter();
      expect(
        () => PropertyCodec.encode(writer, const [
          TopicAlias(0),
        ], MqttPropertyContext.publish),
        throwsA(isA<MqttProtocolException>()),
      );
    });

    test('rejects PayloadFormatIndicator of 2 on decode', () {
      // 0x01 id, value 2
      final bytes = Uint8List.fromList([
        0x02, // property length
        0x01, // Payload Format Indicator
        0x02, // invalid value
      ]);
      expect(
        () => PropertyCodec.decode(
          MqttReader(bytes),
          MqttPropertyContext.publish,
        ),
        throwsA(isA<MqttProtocolException>()),
      );
    });

    test('rejects unknown property identifier', () {
      final bytes = Uint8List.fromList([
        0x01, // property length
        0x7F, // unknown identifier
      ]);
      expect(
        () => PropertyCodec.decode(
          MqttReader(bytes),
          MqttPropertyContext.publish,
        ),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('rejects property length exceeding available bytes', () {
      final bytes = Uint8List.fromList([
        0x05, // property length 5, but only 1 byte follows
        0x01,
      ]);
      expect(
        () => PropertyCodec.decode(
          MqttReader(bytes),
          MqttPropertyContext.publish,
        ),
        throwsA(isA<MqttIncompletePacketException>()),
      );
    });

    test('rejects duplicate non-repeatable on decode', () {
      // Receive Maximum twice (id 0x21)
      final bytes = Uint8List.fromList([
        0x06, // property length
        0x21, 0x00, 0x0A, // Receive Maximum = 10
        0x21, 0x00, 0x14, // Receive Maximum = 20
      ]);
      expect(
        () => PropertyCodec.decode(
          MqttReader(bytes),
          MqttPropertyContext.connack,
        ),
        throwsA(isA<MqttProtocolException>()),
      );
    });

    test('rejects property not allowed in context on decode', () {
      // Session Expiry Interval (0x11) in a PUBLISH context
      final bytes = Uint8List.fromList([
        0x05, // property length
        0x11, 0x00, 0x00, 0x00, 0x01,
      ]);
      expect(
        () => PropertyCodec.decode(
          MqttReader(bytes),
          MqttPropertyContext.publish,
        ),
        throwsA(isA<MqttProtocolException>()),
      );
    });
  });

  group('PropertyCodec golden bytes', () {
    test('encodes a single Payload Format Indicator', () {
      final writer = MqttWriter();
      PropertyCodec.encode(writer, const [
        PayloadFormatIndicator(1),
      ], MqttPropertyContext.publish);
      expect(writer.toBytes(), [0x02, 0x01, 0x01]);
    });

    test('encodes User Property with multi-byte UTF-8', () {
      final writer = MqttWriter();
      PropertyCodec.encode(writer, const [
        UserProperty('k', 'v'),
      ], MqttPropertyContext.publish);
      // id 0x26, then "k" (00 01 6b) then "v" (00 01 76)
      expect(writer.toBytes(), [
        0x07, // property length
        0x26,
        0x00, 0x01, 0x6B,
        0x00, 0x01, 0x76,
      ]);
    });

    test('empty property list encodes a single zero byte', () {
      final writer = MqttWriter();
      PropertyCodec.encode(writer, const [], MqttPropertyContext.publish);
      expect(writer.toBytes(), [0x00]);
    });
  });
}

void _expectRoundTrip(MqttProperty property, MqttPropertyContext context) {
  final effective = context == MqttPropertyContext.connack
      ? property.meta.allowedPackets.first
      : context;
  final writer = MqttWriter();
  PropertyCodec.encode(writer, [property], effective);
  final decoded =
      PropertyCodec.decode(MqttReader(writer.toBytes()), effective);
  expect(decoded, hasLength(1), reason: property.propertyName);
  expect(decoded.single, property, reason: property.propertyName);
}
