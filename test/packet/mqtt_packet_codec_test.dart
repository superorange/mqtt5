import 'dart:typed_data';

import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:mqtt5/src/mqtt_qos.dart';
import 'package:mqtt5/src/packet/auth.dart';
import 'package:mqtt5/src/packet/connack.dart';
import 'package:mqtt5/src/packet/connect.dart';
import 'package:mqtt5/src/packet/disconnect.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/packet/mqtt_reason_code.dart';
import 'package:mqtt5/src/packet/pingreq.dart';
import 'package:mqtt5/src/packet/pingresp.dart';
import 'package:mqtt5/src/packet/puback.dart';
import 'package:mqtt5/src/packet/pubcomp.dart';
import 'package:mqtt5/src/packet/publish.dart';
import 'package:mqtt5/src/packet/pubrec.dart';
import 'package:mqtt5/src/packet/pubrel.dart';
import 'package:mqtt5/src/packet/suback.dart';
import 'package:mqtt5/src/packet/subscribe.dart';
import 'package:mqtt5/src/packet/unsuback.dart';
import 'package:mqtt5/src/packet/unsubscribe.dart';
import 'package:mqtt5/src/property/mqtt_property.dart';
import 'package:mqtt5/src/subscription.dart';
import 'package:test/test.dart';

Uint8List _encode(Object packet) =>
    MqttPacketCodec.encode(packet as dynamic);

void main() {
  group('golden bytes', () {
    test('CONNECT', () {
      final packet = MqttConnectPacket(
        clientId: 'cid',
        cleanStart: true,
        keepAliveSeconds: 60,
      );
      expect(_encode(packet), [
        0x10, 0x10,
        0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, // "MQTT"
        0x05, // protocol level 5
        0x02, // clean start
        0x00, 0x3C, // keep alive 60
        0x00, // no properties
        0x00, 0x03, 0x63, 0x69, 0x64, // "cid"
      ]);
    });

    test('CONNACK session present', () {
      const packet = MqttConnackPacket(
        sessionPresent: true,
        reasonCode: MqttReasonCode.success,
      );
      expect(_encode(packet), [
        0x20, 0x03,
        0x01, // session present
        0x00, // success
        0x00, // no properties
      ]);
    });

    test('PUBLISH qos1', () {
      final packet = MqttPublishPacket(
        topicName: 'a/b',
        payload: Uint8List.fromList([0x01, 0x02]),
        qos: MqttQos.atLeastOnce,
        packetIdentifier: 10,
      );
      expect(_encode(packet), [
        0x32, 0x0A,
        0x00, 0x03, 0x61, 0x2F, 0x62, // "a/b"
        0x00, 0x0A, // packet id 10
        0x00, // no properties
        0x01, 0x02, // payload
      ]);
    });

    test('PUBLISH qos2 retain dup', () {
      final packet = MqttPublishPacket(
        topicName: 't',
        payload: Uint8List.fromList([0xAA]),
        qos: MqttQos.exactlyOnce,
        retain: true,
        dup: true,
        packetIdentifier: 0x0102,
      );
      final bytes = _encode(packet);
      expect(bytes[0], 0x3D); // type 3, dup=1 qos=2 retain=1
      expect(bytes, [
        0x3D, 0x07,
        0x00, 0x01, 0x74, // "t"
        0x01, 0x02, // packet id
        0x00, // no properties
        0xAA,
      ]);
    });

    test('PUBREL', () {
      const packet = MqttPubrelPacket(
        packetIdentifier: 10,
        reasonCode: MqttReasonCode.success,
      );
      expect(_encode(packet), [0x62, 0x03, 0x00, 0x0A, 0x00]);
    });

    test('SUBSCRIBE', () {
      final packet = MqttSubscribePacket(
        packetIdentifier: 1,
        subscriptions: const [
          MqttSubscription(
            'a/b',
            options: MqttSubscriptionOptions(qos: MqttQos.atLeastOnce),
          ),
        ],
      );
      expect(_encode(packet), [
        0x82, 0x09,
        0x00, 0x01, // packet id 1
        0x00, // no properties
        0x00, 0x03, 0x61, 0x2F, 0x62, // "a/b"
        0x01, // options qos1
      ]);
    });

    test('SUBACK', () {
      final packet = MqttSubackPacket(
        packetIdentifier: 1,
        reasonCodes: const [1],
      );
      expect(_encode(packet), [0x90, 0x04, 0x00, 0x01, 0x00, 0x01]);
    });

    test('UNSUBSCRIBE', () {
      final packet = MqttUnsubscribePacket(
        packetIdentifier: 1,
        topicFilters: const ['a/b'],
      );
      expect(_encode(packet), [
        0xA2, 0x08,
        0x00, 0x01,
        0x00,
        0x00, 0x03, 0x61, 0x2F, 0x62,
      ]);
    });

    test('PINGREQ and PINGRESP', () {
      expect(_encode(const MqttPingreqPacket()), [0xC0, 0x00]);
      expect(_encode(const MqttPingrespPacket()), [0xD0, 0x00]);
    });

    test('DISCONNECT', () {
      const packet = MqttDisconnectPacket(
        reasonCode: MqttReasonCode.success,
      );
      expect(_encode(packet), [0xE0, 0x01, 0x00]);
    });

    test('AUTH continue authentication', () {
      const packet = MqttAuthPacket(
        reasonCode: MqttReasonCode.continueAuthentication,
      );
      expect(_encode(packet), [0xF0, 0x01, 0x18]);
    });
  });

  group('golden decode', () {
    test('decodes CONNACK bytes to fields', () {
      final packet = MqttPacketCodec.decode(
        Uint8List.fromList([
          0x20, 0x06,
          0x00, // no session present
          0x00, // success
          0x03, 0x21, 0x00, 0x0A, // Receive Maximum 10
        ]),
      );
      expect(packet, isA<MqttConnackPacket>());
      final connack = packet as MqttConnackPacket;
      expect(connack.sessionPresent, isFalse);
      expect(connack.reasonCode, MqttReasonCode.success);
      expect(connack.properties, const [ReceiveMaximum(10)]);
    });

    test('decodes PUBLISH bytes to fields', () {
      final packet = MqttPacketCodec.decode(
        Uint8List.fromList([
          0x3D, 0x07,
          0x00, 0x01, 0x74,
          0x01, 0x02,
          0x00,
          0xAA,
        ]),
      );
      expect(packet, isA<MqttPublishPacket>());
      final publish = packet as MqttPublishPacket;
      expect(publish.topicName, 't');
      expect(publish.payload, [0xAA]);
      expect(publish.qos, MqttQos.exactlyOnce);
      expect(publish.retain, isTrue);
      expect(publish.dup, isTrue);
      expect(publish.packetIdentifier, 0x0102);
    });

    test('decodes SUBACK granted qos values', () {
      final packet = MqttPacketCodec.decode(
        Uint8List.fromList([0x90, 0x04, 0x00, 0x01, 0x00, 0x01]),
      );
      final suback = packet as MqttSubackPacket;
      expect(suback.packetIdentifier, 1);
      expect(suback.reasonCodes, [1]);
    });
  });

  group('round trip', () {
    test('encode then decode reproduces original bytes', () {
      final packets = [
        MqttConnectPacket(
          clientId: 'client-1',
          cleanStart: false,
          keepAliveSeconds: 30,
          properties: const [SessionExpiryInterval(3600)],
          username: 'user',
          password: Uint8List.fromList([0x01, 0x02]),
        ),
        MqttConnectPacket(
          clientId: 'with-will',
          will: MqttWill(
            topic: 'will/topic',
            payload: Uint8List.fromList([0xDE, 0xAD]),
            qos: MqttQos.exactlyOnce,
            retain: true,
            properties: const [
              WillDelayInterval(5),
              UserProperty('k', 'v'),
            ],
          ),
        ),
        const MqttConnackPacket(
          sessionPresent: true,
          reasonCode: MqttReasonCode.success,
          properties: [
            ReceiveMaximum(20),
            MaximumQos(1),
            TopicAliasMaximum(10),
            AssignedClientIdentifier('assigned'),
            ServerKeepAlive(90),
          ],
        ),
        MqttPublishPacket(
          topicName: 'device/+/status',
          payload: Uint8List.fromList(List.generate(100, (i) => i)),
          qos: MqttQos.exactlyOnce,
          retain: true,
          packetIdentifier: 42,
          properties: [
            const PayloadFormatIndicator(1),
            const MessageExpiryInterval(120),
            const ContentType('application/json'),
            const ResponseTopic('reply'),
            CorrelationData([1, 2, 3]),
            const TopicAlias(3),
            const UserProperty('a', 'b'),
          ],
        ),
        MqttPublishPacket(
          topicName: 'qos0',
          payload: Uint8List.fromList([0x00]),
        ),
        const MqttPubackPacket(
          packetIdentifier: 7,
          reasonCode: MqttReasonCode.noMatchingSubscribers,
          properties: [ReasonString('nobody home')],
        ),
        const MqttPubrecPacket(
          packetIdentifier: 8,
          reasonCode: MqttReasonCode.success,
        ),
        const MqttPubrelPacket(
          packetIdentifier: 9,
          reasonCode: MqttReasonCode.packetIdentifierNotFound,
        ),
        const MqttPubcompPacket(
          packetIdentifier: 10,
          reasonCode: MqttReasonCode.success,
        ),
        MqttSubscribePacket(
          packetIdentifier: 11,
          subscriptions: const [
            MqttSubscription(
              'device/+/status',
              options: MqttSubscriptionOptions(
                qos: MqttQos.exactlyOnce,
                noLocal: true,
                retainAsPublished: true,
                retainHandling: MqttRetainHandling.doNotSend,
              ),
            ),
            MqttSubscription(r'$share/group/topic/#'),
          ],
        ),
        MqttSubackPacket(
          packetIdentifier: 11,
          reasonCodes: const [2, 0x80],
        ),
        MqttUnsubscribePacket(
          packetIdentifier: 12,
          topicFilters: const ['a', 'b/c'],
        ),
        MqttUnsubackPacket(
          packetIdentifier: 12,
          reasonCodes: const [0, 0x11],
        ),
        const MqttPingreqPacket(),
        const MqttPingrespPacket(),
        const MqttDisconnectPacket(
          reasonCode: MqttReasonCode.disconnectWithWillMessage,
          properties: [ReasonString('bye')],
        ),
        MqttAuthPacket(
          reasonCode: MqttReasonCode.continueAuthentication,
          properties: [
            const AuthenticationMethod('SCRAM'),
            AuthenticationData([0x01]),
          ],
        ),
      ];
      for (final packet in packets) {
        final bytes = _encode(packet);
        final decoded = MqttPacketCodec.decode(bytes);
        expect(MqttPacketCodec.encode(decoded), bytes,
            reason: '${packet.type.name} round trip');
      }
    });
  });

  group('decode rejects malformed packets', () {
    test('unknown packet type', () {
      expect(
        () => MqttPacketCodec.decode(Uint8List.fromList([0x00, 0x00])),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('PUBREL with wrong flags', () {
      expect(
        () => MqttPacketCodec.decode(Uint8List.fromList([0x60, 0x02, 0x00, 0x01])),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('SUBSCRIBE with wrong flags', () {
      expect(
        () => MqttPacketCodec.decode(Uint8List.fromList([0x80, 0x00])),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('PUBLISH with QoS 3', () {
      // type 3, flags 0x6 => qos = 3
      expect(
        () => MqttPacketCodec.decode(Uint8List.fromList([0x36, 0x02, 0x00, 0x00])),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('truncated remaining length', () {
      expect(
        () => MqttPacketCodec.decode(Uint8List.fromList([0x20, 0x05, 0x01])),
        throwsA(isA<MqttIncompletePacketException>()),
      );
    });

    test('CONNACK invalid reason code', () {
      // 0x7F is not a valid CONNACK reason code
      expect(
        () => MqttPacketCodec.decode(Uint8List.fromList([0x20, 0x02, 0x00, 0x7F])),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('CONNECT invalid protocol name', () {
      // "NOPE" protocol name
      expect(
        () => MqttPacketCodec.decode(Uint8List.fromList([
          0x10, 0x0C,
          0x00, 0x04, 0x4E, 0x4F, 0x50, 0x45,
          0x05, 0x02, 0x00, 0x3C, 0x00, 0x00, 0x00,
        ])),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('CONNECT reserved flag set', () {
      expect(
        () => MqttPacketCodec.decode(Uint8List.fromList([
          0x10, 0x0C,
          0x00, 0x04, 0x4D, 0x51, 0x54, 0x54,
          0x05, 0x03, 0x00, 0x3C, 0x00, 0x00, 0x00,
        ])),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('empty SUBSCRIBE', () {
      expect(
        () => MqttPacketCodec.decode(Uint8List.fromList([0x82, 0x03, 0x00, 0x01, 0x00])),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('subscription options reserved bits', () {
      expect(
        () => MqttPacketCodec.decode(Uint8List.fromList([
          0x82, 0x07,
          0x00, 0x01,
          0x00,
          0x00, 0x01, 0x61,
          0x40, // reserved bits set
        ])),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });
  });
}
