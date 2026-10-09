/// Regression tests for the specification conformance round.
///
/// Every group names the normative statement it locks in, so a change that
/// trades one of these away has to argue with the specification rather than
/// with a test name.
library;

import 'dart:async';
import 'dart:typed_data';

import 'package:mqtt5/src/client/mqtt_authenticator.dart';
import 'package:mqtt5/src/client/mqtt_client.dart';
import 'package:mqtt5/src/client/mqtt_connection_state.dart';
import 'package:mqtt5/src/client/mqtt_publish_result.dart';
import 'package:mqtt5/src/client/reconnect_manager.dart';
import 'package:mqtt5/src/codec/mqtt_packet_decoder.dart';
import 'package:mqtt5/src/codec/mqtt_writer.dart';
import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:mqtt5/src/mqtt_qos.dart';
import 'package:mqtt5/src/packet/auth.dart';
import 'package:mqtt5/src/packet/connack.dart';
import 'package:mqtt5/src/packet/connect.dart';
import 'package:mqtt5/src/packet/disconnect.dart';
import 'package:mqtt5/src/packet/mqtt_packet.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/packet/mqtt_reason_code.dart';
import 'package:mqtt5/src/packet/pingreq.dart';
import 'package:mqtt5/src/packet/puback.dart';
import 'package:mqtt5/src/packet/publish.dart';
import 'package:mqtt5/src/packet/pubrec.dart';
import 'package:mqtt5/src/packet/pubrel.dart';
import 'package:mqtt5/src/packet/suback.dart';
import 'package:mqtt5/src/packet/subscribe.dart';
import 'package:mqtt5/src/property/mqtt_property.dart';
import 'package:mqtt5/src/property/property_codec.dart';
import 'package:mqtt5/src/property/property_identifier.dart';
import 'package:mqtt5/src/session/topic_alias.dart';
import 'package:mqtt5/src/subscription.dart';
import 'package:mqtt5/src/transport/memory_transport.dart';
import 'package:test/test.dart';

void main() {
  group('session resume (section 4.4, MQTT-3.3.4-8, MQTT-4.6.0-1)', () {
    test('a smaller Receive Maximum on resume does not wedge the client',
        () async {
      // Before the fix the resume loop awaited a send quota slot per stored
      // publication while the connection loop awaited the resume, so a broker
      // that resumed a session with less quota than the client had in flight
      // left the client in `reconnecting` for good.
      final h = _Harness();
      await h.connect(cleanStart: false, brokerReceiveMaximum: 4);

      for (var i = 0; i < 3; i++) {
        h.publish('t/$i', qos: MqttQos.atLeastOnce);
      }
      await _waitFor(() => h.client.inflightCount == 3);
      await h.packets();

      await h.reconnect(sessionPresent: true, brokerReceiveMaximum: 1);
      expect(h.client.state, MqttConnectionState.connected);

      // MQTT-4.9.0-2: only the single slot of quota may be spent.
      final first = await h.packets();
      final resent = first.whereType<MqttPublishPacket>().toList();
      expect(resent, hasLength(1));
      expect(resent.single.dup, isTrue);
      expect(h.client.inflightCount, 3);

      // As the broker acknowledges, the rest follows without a reconnect.
      h.inject(
          MqttPubackPacket(packetIdentifier: resent.single.packetIdentifier));
      final second = await h.packets();
      expect(second.whereType<MqttPublishPacket>(), hasLength(1));

      await h.close();
    });

    test('PUBREL is replayed without spending send quota (MQTT-3.3.4-8)',
        () async {
      final h = _Harness();
      // Room for both publications now, so both reach the session store.
      await h.connect(cleanStart: false, brokerReceiveMaximum: 2);

      h.publish('q2', qos: MqttQos.exactlyOnce);
      final publish = await h.packet<MqttPublishPacket>();
      // Drive that exchange into the PUBREL phase.
      h.inject(MqttPubrecPacket(packetIdentifier: publish.packetIdentifier));
      await h.packet<MqttPubrelPacket>();

      h.publish('q1', qos: MqttQos.atLeastOnce);
      await _waitFor(() => h.client.inflightCount == 2);
      await h.packets();

      // On resume there is exactly one slot of quota. It has to go to the
      // PUBLISH; the PUBREL must go out regardless.
      await h.reconnect(sessionPresent: true, brokerReceiveMaximum: 1);

      final sent = await h.packets();
      expect(sent.whereType<MqttPubrelPacket>(), hasLength(1),
          reason: 'PUBREL must not be delayed by flow control');
      final republished = sent.whereType<MqttPublishPacket>().toList();
      expect(republished, hasLength(1));
      expect(republished.single.topicName, 'q1');
      expect(h.client.state, MqttConnectionState.connected);

      await h.close();
    });

    test('re-sends follow the original publish order (MQTT-4.6.0-1)', () async {
      final h = _Harness();
      await h.connect(cleanStart: false, brokerReceiveMaximum: 10);

      // Interleave QoS levels: the two session stores on their own would
      // replay every QoS 1 entry before any QoS 2 entry.
      for (final (topic, qos) in const [
        ('a', MqttQos.atLeastOnce),
        ('b', MqttQos.exactlyOnce),
        ('c', MqttQos.atLeastOnce),
        ('d', MqttQos.exactlyOnce),
      ]) {
        h.publish(topic, qos: qos);
        await _settle();
      }
      await _waitFor(() => h.client.inflightCount == 4);
      await h.packets();

      await h.reconnect(sessionPresent: true, brokerReceiveMaximum: 10);

      final resent = (await h.packets())
          .whereType<MqttPublishPacket>()
          .map((p) => p.topicName)
          .toList();
      expect(resent, ['a', 'b', 'c', 'd']);

      await h.close();
    });

    test('PUBREL replays follow PUBREC order (MQTT-4.6.0-4)', () async {
      final h = _Harness();
      await h.connect(cleanStart: false, brokerReceiveMaximum: 10);

      h.publish('first', qos: MqttQos.exactlyOnce);
      await _settle();
      h.publish('second', qos: MqttQos.exactlyOnce);
      await _settle();
      final published =
          (await h.packets()).whereType<MqttPublishPacket>().toList();
      expect(published, hasLength(2));

      // The broker answers the second publication first, so PUBREC order and
      // publish order disagree.
      h.inject(
          MqttPubrecPacket(packetIdentifier: published[1].packetIdentifier));
      await _settle();
      h.inject(
          MqttPubrecPacket(packetIdentifier: published[0].packetIdentifier));
      await h.packets();

      await h.reconnect(sessionPresent: true, brokerReceiveMaximum: 10);

      final replayed = (await h.packets())
          .whereType<MqttPubrelPacket>()
          .map((p) => p.packetIdentifier)
          .toList();
      expect(replayed,
          [published[1].packetIdentifier, published[0].packetIdentifier]);

      await h.close();
    });
  });

  group('packets a client must never receive (Table 2-1, section 4.13)', () {
    final invalid = <String, MqttPacket>{
      'PINGREQ': const MqttPingreqPacket(),
      'CONNACK': const MqttConnackPacket(sessionPresent: false),
      'SUBSCRIBE': MqttSubscribePacket(
        packetIdentifier: 7,
        subscriptions: const [MqttSubscription('a/b')],
      ),
    };

    for (final entry in invalid.entries) {
      test('${entry.key} from the broker is a protocol error', () async {
        final h = _Harness(autoReconnect: false);
        h.client.errors.listen((_) {});
        await h.connect();
        await h.packets();

        h.inject(entry.value);
        await _waitFor(() => h.client.metrics.protocolErrorCount > 0);

        // Section 4.13: say why before closing.
        final replies =
            (await h.packets()).whereType<MqttDisconnectPacket>().toList();
        expect(replies, hasLength(1));
        expect(replies.single.reasonCode, MqttReasonCode.protocolError);

        await h.close();
      });
    }

    test('a packet before CONNACK fails the connect (MQTT-3.2.0-1)', () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        transportFactory: () => transport,
        autoReconnect: false,
      );
      final connecting = client.connect();
      await _waitFor(() => transport.outgoing.isNotEmpty);
      transport.takeOutgoingBytes();

      transport.inject(MqttPacketCodec.encode(MqttPublishPacket(
        topicName: 'early',
        payload: Uint8List.fromList([1]),
      )));

      await expectLater(connecting, throwsA(isA<MqttProtocolException>()));
      await client.close();
    });

    test('a stray SUBACK does not free an identifier a publish owns', () async {
      final h = _Harness();
      await h.connect();

      h.publish('t', qos: MqttQos.exactlyOnce);
      final publish = await h.packet<MqttPublishPacket>();
      final id = publish.packetIdentifier;

      h.inject(MqttSubackPacket(packetIdentifier: id, reasonCodes: const [0]));
      await _settle();

      // The exchange is untouched: the identifier is still reserved, so the
      // QoS 2 flow continues instead of colliding with a reuse.
      h.inject(MqttPubrecPacket(packetIdentifier: id));
      final pubrel = await h.packet<MqttPubrelPacket>();
      expect(pubrel.packetIdentifier, id);
      expect(h.client.inflightCount, 1);

      await h.close();
    });
  });

  group('CONNACK semantics (section 3.2.2.1.1)', () {
    test('Session Present after a Clean Start is rejected (MQTT-3.2.2-2)',
        () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        transportFactory: () => transport,
        autoReconnect: false,
      );
      final connecting = client.connect();
      await _waitFor(() => transport.outgoing.isNotEmpty);
      transport.takeOutgoingBytes();
      transport.inject(MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: true),
      ));

      await expectLater(connecting, throwsA(isA<MqttProtocolException>()));
      // Section 4.13: the broker is told why before the socket goes away.
      final farewell = MqttPacketDecoder()
          .feed(transport.takeOutgoingBytes())
          .whereType<MqttDisconnectPacket>()
          .toList();
      expect(farewell, hasLength(1));
      expect(farewell.single.reasonCode, MqttReasonCode.protocolError);
      await client.close();
    });

    test('the granted Session Expiry Interval is reported, not adopted',
        () async {
      final h = _Harness();
      await h.connect(
        cleanStart: false,
        sessionExpiryInterval: const Duration(hours: 1),
        extraProperties: const [SessionExpiryInterval(300)],
      );

      expect(h.client.serverCapabilities.sessionExpiryInterval,
          const Duration(seconds: 300));

      // The next CONNECT still asks for what the application configured.
      await h.reconnect(sessionPresent: true);
      final requested =
          h.lastConnect!.properties.whereType<SessionExpiryInterval>().single;
      expect(requested.seconds, const Duration(hours: 1).inSeconds);

      await h.close();
    });
  });

  group('topic syntax (section 4.7, 4.8.2)', () {
    late _Harness h;
    setUp(() async {
      h = _Harness();
      await h.connect();
    });
    tearDown(() => h.close());

    test('publish rejects a wildcard or empty topic name', () {
      expect(() => h.client.publish('a/+/b', _payload), throwsArgumentError);
      expect(() => h.client.publish('a/#', _payload), throwsArgumentError);
      expect(() => h.client.publish('', _payload), throwsArgumentError);
    });

    test('subscribe rejects malformed filters', () {
      for (final bad in [
        'sport/tennis#',
        'sport/#/ranking',
        'sport+',
        '',
        r'$share/group',
        r'$share//topic',
        r'$share/a+b/x',
      ]) {
        expect(() => h.client.subscribe(bad), throwsArgumentError, reason: bad);
      }
    });

    test('subscribe still accepts the legal wildcard forms', () async {
      final pending = h.client.subscribe(r'$share/g/sport/+/score/#');
      final subscribe = await h.packet<MqttSubscribePacket>();
      h.inject(MqttSubackPacket(
        packetIdentifier: subscribe.packetIdentifier,
        reasonCodes: const [0],
      ));
      await pending;
    });

    test('unsubscribe rejects malformed filters', () {
      expect(() => h.client.unsubscribe(['a/#/b']), throwsArgumentError);
    });

    test('a will topic with a wildcard is rejected at construction', () {
      expect(
        () => MqttClient(
          host: 'h',
          will: MqttWill(topic: 'a/+/b', payload: _payload),
        ),
        throwsArgumentError,
      );
    });

    test('a Response Topic with a wildcard is a protocol error', () {
      expect(
        () => PropertyCodec.encode(
          MqttWriter(),
          const [ResponseTopic('reply/+')],
          MqttPropertyContext.publish,
        ),
        throwsA(isA<MqttProtocolException>()),
      );
      // A plain topic name still round-trips.
      expect(
        () => PropertyCodec.encode(
          MqttWriter(),
          const [ResponseTopic('reply/here')],
          MqttPropertyContext.publish,
        ),
        returnsNormally,
      );
    });
  });

  group('topic alias (section 3.3.2.3)', () {
    test('rebinding an alias drops the stale reverse entry', () {
      final map = TopicAliasMap(maximum: 2);
      map.register(1, 'a');
      map.register(1, 'b');
      expect(map.resolve(1), 'b');
      expect(map.aliasFor('a'), isNull);
      expect(map.aliasFor('b'), 1);
    });

    test('cycling one alias through many topics stays bounded', () {
      final map = TopicAliasMap(maximum: 1);
      for (var i = 0; i < 1000; i++) {
        map.register(1, 'topic/$i');
      }
      expect(map.resolve(1), 'topic/999');
      expect(map.aliasFor('topic/0'), isNull);
    });

    test('two aliases may name the same topic', () {
      final map = TopicAliasMap(maximum: 2);
      map.register(1, 'a');
      map.register(2, 'a');
      expect(map.resolve(1), 'a', reason: 'the older alias stays resolvable');
      expect(map.resolve(2), 'a');
    });

    test('a caller supplied alias above the negotiated maximum is rejected',
        () async {
      final h = _Harness();
      await h.connect(extraProperties: const [TopicAliasMaximum(5)]);

      expect(
        () =>
            h.client.publish('t', _payload, properties: const [TopicAlias(6)]),
        throwsA(isA<MqttFlowControlException>()),
      );
      // Within the negotiated range it is passed through untouched.
      await h.client.publish('t', _payload, properties: const [TopicAlias(5)]);

      await h.close();
    });

    test('with no negotiated alias space any supplied alias is rejected',
        () async {
      final h = _Harness();
      await h.connect();
      expect(
        () =>
            h.client.publish('t', _payload, properties: const [TopicAlias(1)]),
        throwsA(isA<MqttFlowControlException>()),
      );
      await h.close();
    });
  });

  group('enhanced authentication (section 4.12)', () {
    test('reauthenticate completes on AUTH 0x00 (MQTT-4.12.1-1)', () async {
      final h = _Harness(authenticator: _StaticAuthenticator());
      await h.connect(authenticationMethod: 'SCRAM-SHA-1');
      await h.packets();

      final reauth =
          h.client.reauthenticate(authenticationData: Uint8List.fromList([9]));
      final sent = await h.packet<MqttAuthPacket>();
      expect(sent.reasonCode, MqttReasonCode.reAuthenticate);
      expect(sent.properties.whereType<AuthenticationMethod>().single.value,
          'SCRAM-SHA-1');
      expect(h.client.state, MqttConnectionState.authenticating);

      h.inject(const MqttAuthPacket(
        reasonCode: MqttReasonCode.continueAuthentication,
        properties: [AuthenticationMethod('SCRAM-SHA-1')],
      ));
      // MQTT-4.12.0-3: the client always answers with 0x18.
      final response = await h.packet<MqttAuthPacket>();
      expect(response.reasonCode, MqttReasonCode.continueAuthentication);
      expect(response.properties.whereType<AuthenticationMethod>().single.value,
          'SCRAM-SHA-1');

      h.inject(const MqttAuthPacket(
        reasonCode: MqttReasonCode.success,
        properties: [AuthenticationMethod('SCRAM-SHA-1')],
      ));
      await reauth;
      expect(h.client.state, MqttConnectionState.connected);

      await h.close();
    });

    test('publishing keeps working during re-authentication', () async {
      final h = _Harness(authenticator: _StaticAuthenticator());
      await h.connect(authenticationMethod: 'SCRAM-SHA-1');
      await h.packets();

      final reauth = h.client.reauthenticate();
      await h.packet<MqttAuthPacket>();
      // Section 4.12.1: other packets continue to flow throughout.
      await h.client.publish('during/reauth', _payload);

      h.inject(const MqttAuthPacket(
        reasonCode: MqttReasonCode.success,
        properties: [AuthenticationMethod('SCRAM-SHA-1')],
      ));
      await reauth;
      await h.close();
    });

    test('reauthenticate needs an authentication method (MQTT-4.12.0-7)',
        () async {
      final h = _Harness();
      await h.connect();
      await expectLater(h.client.reauthenticate(),
          throwsA(isA<MqttAuthenticationException>()));
      await h.close();
    });

    test(
        'a broker AUTH that switches method is a protocol error '
        '(MQTT-4.12.0-5)', () async {
      final h = _Harness(authenticator: _StaticAuthenticator());
      await h.connect(authenticationMethod: 'SCRAM-SHA-1');
      await h.packets();

      h.inject(const MqttAuthPacket(
        reasonCode: MqttReasonCode.continueAuthentication,
        properties: [AuthenticationMethod('PLAIN')],
      ));
      await _waitFor(() => h.client.metrics.protocolErrorCount > 0);
      await h.close();
    });

    test(
        'a broker AUTH with no agreed method is a protocol error '
        '(MQTT-4.12.0-6)', () async {
      final h = _Harness(authenticator: _StaticAuthenticator());
      await h.connect();
      await h.packets();

      h.inject(const MqttAuthPacket(
          reasonCode: MqttReasonCode.continueAuthentication));
      await _waitFor(() => h.client.metrics.protocolErrorCount > 0);
      await h.close();
    });

    test('a broker AUTH claiming reason code 0x19 is a protocol error',
        () async {
      final h = _Harness(authenticator: _StaticAuthenticator());
      await h.connect(authenticationMethod: 'SCRAM-SHA-1');
      await h.packets();

      // Table 3-11 marks 0x19 as sent by the Client only.
      h.inject(const MqttAuthPacket(
        reasonCode: MqttReasonCode.reAuthenticate,
        properties: [AuthenticationMethod('SCRAM-SHA-1')],
      ));
      await _waitFor(() => h.client.metrics.protocolErrorCount > 0);
      await h.close();
    });
  });

  group('DISCONNECT (section 3.14.2.2.2)', () {
    test('extending a zero Session Expiry Interval is rejected', () async {
      final h = _Harness();
      await h.connect();
      expect(
        () =>
            h.client.disconnect(properties: const [SessionExpiryInterval(60)]),
        throwsArgumentError,
      );
      await h.close();
    });

    test('it is allowed when CONNECT asked for a non-zero interval', () async {
      final h = _Harness();
      await h.connect(
        cleanStart: false,
        sessionExpiryInterval: const Duration(seconds: 30),
      );
      await h.client.disconnect(properties: const [SessionExpiryInterval(60)]);
      await h.close();
    });

    test('an interval supplied through connect properties counts too',
        () async {
      // The rule is about what the CONNECT packet carried, and the interval
      // can be set either way round.
      final transport = MemoryTransport();
      final client = MqttClient(host: 'h', transportFactory: () => transport);
      final connecting = client.connect(
        cleanStart: false,
        properties: const [SessionExpiryInterval(30)],
      );
      await _waitFor(() => transport.outgoing.isNotEmpty);
      transport.takeOutgoingBytes();
      transport.inject(MqttPacketCodec.encode(
          const MqttConnackPacket(sessionPresent: false)));
      await connecting;

      await client.disconnect(properties: const [SessionExpiryInterval(60)]);
      await client.close();
    });
  });

  group('reason codes (Table 2-4)', () {
    test('Granted QoS 1 and Granted QoS 2 are known values', () {
      expect(MqttReasonCode.tryFromValue(0x01), MqttReasonCode.grantedQos1);
      expect(MqttReasonCode.tryFromValue(0x02), MqttReasonCode.grantedQos2);
    });

    test('every reason code a packet may carry maps to a known name', () {
      final all = <int>{
        ...connackReasonCodes,
        ...pubackReasonCodes,
        ...pubrelReasonCodes,
        ...subackReasonCodes,
        ...unsubackReasonCodes,
        ...disconnectReasonCodes,
        ...authReasonCodes,
      };
      for (final value in all) {
        expect(MqttReasonCode.tryFromValue(value), isNotNull,
            reason: '0x${value.toRadixString(16)}');
      }
    });
  });

  test('MqttPropertyIdentifier matches the property metadata registry', () {
    // The enum is public API and the registry drives the codec. They are
    // separate declarations, so they are checked against each other rather
    // than trusted to stay in step.
    expect(
      MqttPropertyIdentifier.values.map((e) => e.value).toSet(),
      mqttPropertyMetaById.keys.toSet(),
    );
  });

  group('audited spec conformance & edge case fixes', () {
    test('TopicAliasMap LRU eviction when enableEviction is true', () {
      final map = TopicAliasMap(enableEviction: true);
      map.maximum = 2;

      final a1 = map.reserve()!;
      expect(a1, 1);
      map.commit(1, 'topic/1');

      final a2 = map.reserve()!;
      expect(a2, 2);
      map.commit(2, 'topic/2');

      // Access topic/1 so topic/2 becomes LRU
      expect(map.aliasFor('topic/1'), 1);

      // Reserve topic/3: evicts topic/2 (alias 2)
      final a3 = map.reserve()!;
      expect(a3, 2);
      map.commit(2, 'topic/3');

      expect(map.resolve(2), 'topic/3');
      expect(map.resolve(1), 'topic/1');
      expect(map.aliasFor('topic/2'), isNull);
    });

    test('TopicAliasMap does not evict when enableEviction is false', () {
      final map = TopicAliasMap(enableEviction: false);
      map.maximum = 2;

      final a1 = map.reserve()!;
      map.commit(a1, 'topic/1');
      final a2 = map.reserve()!;
      map.commit(a2, 'topic/2');

      expect(map.reserve(), isNull);
    });

    test(
        'client topicAliasEviction allows publishing more topics than server TopicAliasMaximum',
        () async {
      final h = _Harness(topicAliasEviction: true);
      await h.connect(
        cleanStart: true,
        extraProperties: const [TopicAliasMaximum(2)],
      );

      // Publish topic 1 (assigns alias 1)
      h.publish('t/1', qos: MqttQos.atMostOnce);
      final p1 = await h.packet<MqttPublishPacket>();
      expect(p1.topicName, 't/1');
      expect(p1.properties, contains(const TopicAlias(1)));

      // Publish topic 2 (assigns alias 2)
      h.publish('t/2', qos: MqttQos.atMostOnce);
      final p2 = await h.packet<MqttPublishPacket>();
      expect(p2.topicName, 't/2');
      expect(p2.properties, contains(const TopicAlias(2)));

      // Publish topic 1 again (uses alias 1 without topic name)
      h.publish('t/1', qos: MqttQos.atMostOnce);
      final p1again = await h.packet<MqttPublishPacket>();
      expect(p1again.topicName, '');
      expect(p1again.properties, contains(const TopicAlias(1)));

      // Publish topic 3: evicts topic 2 (alias 2) and sends full topic name
      h.publish('t/3', qos: MqttQos.atMostOnce);
      final p3 = await h.packet<MqttPublishPacket>();
      expect(p3.topicName, 't/3');
      expect(p3.properties, contains(const TopicAlias(2)));

      await h.close();
    });

    test('reconnect sends cleanStart: false when sessionExpiryInterval > 0',
        () async {
      final h = _Harness();
      await h.connect(
        cleanStart: true,
        sessionExpiryInterval: const Duration(seconds: 300),
      );
      expect(h.lastConnect?.cleanStart, isTrue);

      final before = h.transports.length;
      h.transport.injectError(MqttTransportException('simulated drop'));
      await _waitFor(() => h.transports.length > before);

      final reconnectConnect = await h.packet<MqttConnectPacket>();
      expect(reconnectConnect.cleanStart, isFalse,
          reason:
              'Auto-reconnect with non-zero session expiry must attempt resume');

      h.inject(const MqttConnackPacket(sessionPresent: true));
      await _waitFor(() => h.client.state == MqttConnectionState.connected);
      expect(h.client.state, MqttConnectionState.connected);
      await h.close();
    });

    test('reconnectCleanStart parameter overrides default reconnect behavior',
        () async {
      final h = _Harness();
      await h.connect(
        cleanStart: true,
        reconnectCleanStart: true,
        sessionExpiryInterval: const Duration(seconds: 300),
      );
      expect(h.lastConnect?.cleanStart, isTrue);

      final before = h.transports.length;
      h.transport.injectError(MqttTransportException('simulated drop'));
      await _waitFor(() => h.transports.length > before);

      final reconnectConnect = await h.packet<MqttConnectPacket>();
      expect(reconnectConnect.cleanStart, isTrue,
          reason: 'Explicit reconnectCleanStart: true must be respected');

      h.inject(const MqttConnackPacket(sessionPresent: false));
      await _waitFor(() => h.client.state == MqttConnectionState.connected);
      expect(h.client.state, MqttConnectionState.connected);
      await h.close();
    });

    test('reauthenticate timeout aborts connection [MQTT-4.12.1-2]', () async {
      final h = _Harness(
        autoReconnect: false,
        operationTimeout: const Duration(milliseconds: 50),
      );
      await h.connect(
        cleanStart: true,
        authenticationMethod: 'SCRAM-SHA-256',
      );

      expect(h.client.state, MqttConnectionState.connected);
      final t = h.transport;

      // Reauthenticate: will send AUTH 0x19 and time out in 50ms
      unawaited(
        h.client
            .reauthenticate(
              authenticationData: Uint8List.fromList([1, 2]),
            )
            .catchError((Object _) {}),
      );

      final authPacket = await h.packet<MqttAuthPacket>();
      expect(authPacket.reasonCode, MqttReasonCode.reAuthenticate);

      // On timeout, it must send DISCONNECT and close. 0x87 is reserved for
      // the server (Table 3-10), so the client says 0x80.
      final disconnect = await h.packet<MqttDisconnectPacket>();
      expect(disconnect.reasonCode, MqttReasonCode.unspecifiedError);
      await _waitFor(() =>
          t.isClosed || h.client.state == MqttConnectionState.disconnected);
      expect(h.client.state, MqttConnectionState.disconnected);
      await h.close();
    });

    test(
        'inbound QoS 1 exceeds Receive Maximum when QoS 2 fills quota [MQTT-3.3.4-9]',
        () async {
      final h = _Harness(autoReconnect: false);
      await h.connect(
        cleanStart: true,
        clientReceiveMaximum: 1,
      );
      // Acknowledgements are only sent once a message reaches a listener.
      h.client.messages.listen((_) {});

      // 1 QoS 2 publish fills inbound quota of 1
      h.inject(MqttPublishPacket(
        topicName: 't/qos2',
        packetIdentifier: 10,
        qos: MqttQos.exactlyOnce,
        payload: Uint8List.fromList([1]),
      ));
      await h.packet<MqttPubrecPacket>();

      // Broker sends QoS 1 publish while QoS 2 is unacknowledged
      h.inject(MqttPublishPacket(
        topicName: 't/qos1',
        packetIdentifier: 11,
        qos: MqttQos.atLeastOnce,
        payload: Uint8List.fromList([2]),
      ));

      // Client must send DISCONNECT 0x93 (receiveMaximumExceeded) and close
      final disconnect = await h.packet<MqttDisconnectPacket>();
      expect(disconnect.reasonCode, MqttReasonCode.receiveMaximumExceeded);
      await h.close();
    });

    test('delayed SUBACK after timeout still registers granted subscriptions',
        () async {
      final h = _Harness(operationTimeout: const Duration(milliseconds: 50));
      await h.connect(cleanStart: true);

      // Subscribe and let it time out
      unawaited(
        h.client.subscribe('delayed/topic').catchError((Object _) {}),
      );
      final subPacket = await h.packet<MqttSubscribePacket>();

      // Wait past timeout
      await Future<void>.delayed(const Duration(milliseconds: 60));

      // Broker responds with delayed SUBACK
      h.inject(MqttSubackPacket(
        packetIdentifier: subPacket.packetIdentifier,
        reasonCodes: const [0],
      ));
      await _settle();

      // Trigger session loss reconnect (sessionPresent: false)
      final before = h.transports.length;
      h.transport.injectError(MqttTransportException('simulated drop'));
      await _waitFor(() => h.transports.length > before);
      await h.packet<MqttConnectPacket>();
      h.inject(const MqttConnackPacket(sessionPresent: false));

      // Resubscribe should now include 'delayed/topic'
      final resub = await h.packet<MqttSubscribePacket>();
      expect(resub.subscriptions.map((s) => s.topicFilter),
          contains('delayed/topic'));
      await h.close();
    });
  });
}

final Uint8List _payload = Uint8List.fromList([1]);

/// Drives one client over in-memory transports, with the reconnect step
/// spelled out because several of these tests turn on what the client does
/// across a resumed session.
final class _Harness {
  _Harness({
    this.authenticator,
    this.autoReconnect = true,
    this.operationTimeout = const Duration(seconds: 30),
    this.topicAliasEviction = false,
  });

  final MqttAuthenticator? authenticator;

  /// Off for the tests that inspect what the client wrote on the way out: a
  /// reconnect installs a fresh transport, and the farewell DISCONNECT is on
  /// the old one.
  final bool autoReconnect;
  final Duration operationTimeout;
  final bool topicAliasEviction;

  final List<MemoryTransport> transports = <MemoryTransport>[];

  late final MqttClient client = MqttClient(
    host: 'h',
    transportFactory: _make,
    authenticator: authenticator,
    autoReconnect: autoReconnect,
    operationTimeout: operationTimeout,
    topicAliasEviction: topicAliasEviction,
    reconnectManager: ReconnectManager(
      initialDelay: const Duration(milliseconds: 5),
      maxDelay: const Duration(milliseconds: 5),
      jitterFactor: 0,
    ),
  );

  MqttConnectPacket? lastConnect;

  /// Kept so a reconnect asks for the same session settings the first
  /// connection did.
  bool _cleanStart = true;

  MemoryTransport get transport => transports.last;

  MemoryTransport _make() {
    final t = MemoryTransport();
    transports.add(t);
    return t;
  }

  Future<void> connect({
    bool cleanStart = true,
    bool? reconnectCleanStart,
    int? clientReceiveMaximum,
    int? brokerReceiveMaximum,
    Duration? sessionExpiryInterval,
    String? authenticationMethod,
    List<MqttProperty> extraProperties = const [],
  }) async {
    _cleanStart = cleanStart;
    final connecting = client.connect(
      cleanStart: cleanStart,
      reconnectCleanStart: reconnectCleanStart,
      receiveMaximum: clientReceiveMaximum ?? 65535,
      sessionExpiryInterval: sessionExpiryInterval,
      authenticationMethod: authenticationMethod,
    );
    lastConnect = await packet<MqttConnectPacket>();
    inject(MqttConnackPacket(
      sessionPresent: false,
      properties: [
        if (brokerReceiveMaximum != null) ReceiveMaximum(brokerReceiveMaximum),
        ...extraProperties,
      ],
    ));
    await connecting;
  }

  /// Drops the connection and completes the handshake of the one that follows.
  Future<void> reconnect({
    required bool sessionPresent,
    int? brokerReceiveMaximum,
  }) async {
    expect(_cleanStart, isFalse,
        reason: 'a resumed session needs cleanStart: false');
    final before = transports.length;
    transport.injectError(MqttTransportException('simulated drop'));
    await _waitFor(() => transports.length > before);
    lastConnect = await packet<MqttConnectPacket>();
    inject(MqttConnackPacket(
      sessionPresent: sessionPresent,
      properties: [
        if (brokerReceiveMaximum != null) ReceiveMaximum(brokerReceiveMaximum),
      ],
    ));
    await _waitFor(() => client.state == MqttConnectionState.connected);
    await _settle();
  }

  /// Publishes without awaiting: these tests deliberately leave publications
  /// unacknowledged, and an unhandled rejection at teardown would fail the
  /// test for the wrong reason.
  void publish(String topic, {required MqttQos qos}) {
    unawaited(
      client
          .publish(topic, _payload, qos: qos)
          .catchError((Object _) => const MqttPublishResult()),
    );
  }

  void inject(MqttPacket packet) {
    transport.inject(MqttPacketCodec.encode(packet));
  }

  /// Everything the client has written since the last call.
  Future<List<MqttPacket>> packets() async {
    await _settle();
    final bytes = transport.takeOutgoingBytes();
    if (bytes.isEmpty) return const [];
    return MqttPacketDecoder().feed(bytes);
  }

  Future<T> packet<T extends MqttPacket>() async {
    final deadline = DateTime.now().add(const Duration(seconds: 5));
    while (true) {
      for (final packet in await packets()) {
        if (packet is T) return packet;
      }
      if (DateTime.now().isAfter(deadline)) {
        fail('Timed out waiting for a $T from the client');
      }
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
  }

  Future<void> close() => client.close();
}

final class _StaticAuthenticator implements MqttAuthenticator {
  @override
  Future<MqttAuthResponse?> authenticate(MqttAuthChallenge challenge) async =>
      MqttAuthResponse(Uint8List.fromList([1, 2, 3]));
}

Future<void> _settle() async {
  for (var i = 0; i < 8; i++) {
    await Future<void>.delayed(Duration.zero);
  }
}

Future<void> _waitFor(bool Function() predicate) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!predicate()) {
    if (DateTime.now().isAfter(deadline)) fail('timed out');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
