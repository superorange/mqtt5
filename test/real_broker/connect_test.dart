@Tags(['real-broker'])
@Timeout(Duration(seconds: 60))
library;

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'support/common.dart';

void main() {
  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
    return;
  }

  group('CONNECT / CONNACK (real mosquitto)', () {
    late Mosquitto broker;
    tearDown(() => broker.dispose());

    test(
        'plain connect reports connecting -> connected and the broker sees '
        'protocol level 5, clean start and the requested keep alive', () async {
      broker = await Mosquitto.start();
      final client = newClient(broker.port, clientId: 'c-basic');
      final states = <MqttConnectionState>[];
      client.stateStream.listen(states.add);
      await client.connect(keepAlive: const Duration(seconds: 30));
      expect(client.state, MqttConnectionState.connected);
      expect(client.sessionPresent, isFalse);
      expect(states, [
        MqttConnectionState.connecting,
        MqttConnectionState.connected,
      ]);
      await broker.waitForLog('as c-basic (p5, c1, k30)');
      await client.disconnect();
      await broker.waitForLog('Received DISCONNECT from c-basic');
      expect(states.last, MqttConnectionState.disconnected);
      expect(broker.log, isNot(contains('protocol error')));
      await client.close();
    });

    test('CONNACK capabilities from a constrained broker are applied',
        () async {
      broker = await Mosquitto.start(config: [
        'max_inflight_messages 3',
        'max_packet_size 2000',
        'max_qos 1',
        'retain_available false',
        'max_topic_alias 4',
        'max_keepalive 7',
      ]);
      final client = newClient(broker.port, clientId: 'c-caps');
      await client.connect(keepAlive: const Duration(seconds: 60));
      final caps = client.serverCapabilities;
      expect(caps.receiveMaximum, 3);
      expect(caps.maximumPacketSize, 2000);
      expect(caps.maximumQos, 1);
      expect(caps.retainAvailable, isFalse);
      expect(caps.topicAliasMaximum, 4);
      expect(caps.serverKeepAlive, const Duration(seconds: 7));

      // Local enforcement instead of being disconnected by the broker.
      await expectLater(
          client.publish('t', bytes('x'), qos: MqttQos.exactlyOnce),
          throwsA(isA<MqttFlowControlException>()));
      await expectLater(client.publish('t', bytes('x'), retain: true),
          throwsA(isA<MqttFlowControlException>()));
      await expectLater(client.publish('t', Uint8List(3000)),
          throwsA(isA<MqttPacketTooLargeException>()));
      // The connection survives all of the above.
      expect(client.state, MqttConnectionState.connected);
      final r =
          await client.publish('t', bytes('ok'), qos: MqttQos.atLeastOnce);
      expect(r.reasonCode, isNot(MqttReasonCode.unspecifiedError));
      await client.close();
    });

    test(
        'Server Keep Alive overrides the requested keep alive: PINGREQ is '
        'sent on the server schedule and the broker never times us out',
        () async {
      broker = await Mosquitto.start(config: ['max_keepalive 2']);
      final client = newClient(broker.port, clientId: 'c-ska');
      await client.connect(keepAlive: const Duration(seconds: 600));
      expect(client.serverCapabilities.serverKeepAlive,
          const Duration(seconds: 2));
      await Future<void>.delayed(const Duration(milliseconds: 6500));
      expect(broker.countLog('Received PINGREQ from c-ska'),
          greaterThanOrEqualTo(2));
      expect(broker.countLog('Sending PINGRESP to c-ska'),
          greaterThanOrEqualTo(2));
      expect(broker.log, isNot(contains('exceeded timeout')));
      expect(client.state, MqttConnectionState.connected);
      expect(client.metrics.lastPingRtt, isNotNull);
      await client.close();
    });

    test(
        'keep alive 0 with a broker max_keepalive is replaced by the server '
        'value', () async {
      broker = await Mosquitto.start(config: ['max_keepalive 3']);
      final client = newClient(broker.port, clientId: 'c-ka0');
      await client.connect(keepAlive: Duration.zero);
      expect(client.serverCapabilities.serverKeepAlive,
          const Duration(seconds: 3));
      await Future<void>.delayed(const Duration(milliseconds: 5000));
      expect(broker.countLog('Received PINGREQ from c-ka0'),
          greaterThanOrEqualTo(1));
      expect(client.state, MqttConnectionState.connected);
      await client.close();
    });

    test(
        'empty client id: broker-assigned identifier is adopted and reused '
        'on reconnect', () async {
      broker = await Mosquitto.start(config: ['auto_id_prefix zz-']);
      final proxy = await FaultProxy.start(broker.port);
      final client = newClient(proxy.port, clientId: '');
      await client.connect(sessionExpiryInterval: const Duration(seconds: 60));
      final assigned = client.effectiveClientId;
      expect(assigned, startsWith('zz-'));
      expect(
          prop(proxy.received(kConnack).first.ackProperties, 0x12), assigned);

      proxy.cutAll();
      await waitUntil(() =>
          proxy.sent(kConnect).length == 2 &&
          client.state == MqttConnectionState.connected);
      final reconnect = proxy.sent(kConnect).last.connect;
      expect(reconnect.clientId, assigned);
      // Session resumed with the assigned id: clean start bit is 0.
      expect(reconnect.flags & 0x02, 0);
      expect(client.sessionPresent, isTrue);
      await client.close();
      await proxy.close();
    });

    test('username/password accepted', () async {
      broker = await Mosquitto.start(users: {'alice': 's3cret'});
      final client = newClient(broker.port,
          clientId: 'c-auth', username: 'alice', password: 's3cret');
      await client.connect();
      expect(client.state, MqttConnectionState.connected);
      await client.close();
    });

    test(
        'wrong password: CONNACK 0x86/0x87 surfaces once and is not retried even '
        'with autoReconnect', () async {
      broker = await Mosquitto.start(users: {'alice': 's3cret'});
      final client = newClient(broker.port,
          clientId: 'c-badpw', username: 'alice', password: 'nope');
      await expectLater(
        client.connect(),
        throwsA(isA<MqttServerRejectedException>()
            .having((e) => e.reasonCode, 'reasonCode', anyOf(0x86, 0x87))),
      );
      await settle(800);
      expect(broker.countLog('Sending CONNACK to c-badpw'), 1);
      expect(client.state, MqttConnectionState.disconnected);
      await client.close();
    });

    test(
        'anonymous connection to a broker requiring auth is rejected and not '
        'retried', () async {
      broker = await Mosquitto.start(users: {'alice': 's3cret'});
      final client = newClient(broker.port, clientId: 'c-anon');
      Object? error;
      try {
        await client.connect();
      } on Object catch (e) {
        error = e;
      }
      expect(error, isA<MqttServerRejectedException>());
      final code = (error as MqttServerRejectedException).reasonCode;
      expect(code, anyOf(0x86, 0x87));
      await settle(800);
      expect(broker.countLog('Sending CONNACK to c-anon'), 1);
      await client.close();
    });

    test('Authentication Method the broker does not support: 0x8C, fatal',
        () async {
      broker = await Mosquitto.start();
      final client = newClient(broker.port, clientId: 'c-nomethod');
      await expectLater(
        client.connect(authenticationMethod: 'SCRAM-SHA-1'),
        throwsA(isA<MqttServerRejectedException>()
            .having((e) => e.reasonCode, 'reasonCode', 0x8C)),
      );
      await settle(500);
      expect(broker.countLog('Sending CONNACK to c-nomethod'), 1);
      await client.close();
    });

    test('CONNACK timeout when the link swallows everything', () async {
      broker = await Mosquitto.start();
      final proxy = await FaultProxy.start(broker.port)
        ..blackhole = true;
      final client =
          newClient(proxy.port, clientId: 'c-cto', autoReconnect: false);
      final sw = Stopwatch()..start();
      await expectLater(
        client.connect(connackTimeout: const Duration(milliseconds: 700)),
        throwsA(isA<MqttConnectionException>()),
      );
      expect(sw.elapsedMilliseconds, inInclusiveRange(600, 3000));
      expect(client.state, MqttConnectionState.disconnected);
      await client.close();
      await proxy.close();
    });

    test('connection refused with autoReconnect:false fails fast', () async {
      broker = await Mosquitto.start();
      final port = await freePort();
      final client =
          newClient(port, clientId: 'c-refused', autoReconnect: false);
      await expectLater(client.connect(), throwsA(isA<SocketException>()));
      expect(client.state, MqttConnectionState.disconnected);
      await client.close();
    });

    test('connect() keeps retrying until the broker comes up', () async {
      final port = await freePort();
      final client = newClient(port, clientId: 'c-late');
      final connecting = client.connect();
      await settle(700);
      expect(client.state, MqttConnectionState.reconnecting);
      broker = await Mosquitto.start(port: port);
      await connecting.timeout(const Duration(seconds: 5));
      expect(client.state, MqttConnectionState.connected);
      await client.close();
    });

    test(
        'Session Present after reconnecting with Clean Start 0, and QoS 1 '
        'messages queued while offline are delivered', () async {
      broker = await Mosquitto.start();
      final client = newClient(broker.port, clientId: 'c-sess');
      await client.connect(sessionExpiryInterval: const Duration(seconds: 300));
      expect(client.sessionPresent, isFalse);
      await client.subscribe('sess/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      await client.disconnect();

      for (var i = 0; i < 5; i++) {
        await mosqPub(broker.port, 'sess/t', 'm$i', qos: 1);
      }

      final inbox = Inbox(client);
      await client.connect(
          cleanStart: false,
          sessionExpiryInterval: const Duration(seconds: 300));
      expect(client.sessionPresent, isTrue);
      await inbox.waitFor(5);
      expect(inbox.payloads, ['m0', 'm1', 'm2', 'm3', 'm4']);
      expect(inbox.messages.every((m) => m.qos == MqttQos.atLeastOnce), isTrue);
      await client.close();
    });

    test('Session Expiry 0: the session ends with the connection', () async {
      broker = await Mosquitto.start();
      final client = newClient(broker.port, clientId: 'c-sei0');
      await client.connect();
      await client.subscribe('x');
      await client.disconnect();
      await client.connect(cleanStart: false);
      expect(client.sessionPresent, isFalse);
      await client.close();
    });

    test('Clean Start 1 discards an existing broker session', () async {
      broker = await Mosquitto.start();
      final client = newClient(broker.port, clientId: 'c-cs1');
      await client.connect(sessionExpiryInterval: const Duration(seconds: 300));
      await client.subscribe('cs1/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      await client.disconnect();
      await mosqPub(broker.port, 'cs1/t', 'stale', qos: 1);
      final inbox = Inbox(client);
      await client.connect(
          cleanStart: true,
          sessionExpiryInterval: const Duration(seconds: 300));
      expect(client.sessionPresent, isFalse);
      await settle(500);
      expect(inbox.payloads, isEmpty);
      await client.close();
    });

    test(
        'DISCONNECT with Session Expiry 0 ends a session that CONNECT asked '
        'to keep', () async {
      broker = await Mosquitto.start();
      final client = newClient(broker.port, clientId: 'c-dsei');
      await client.connect(sessionExpiryInterval: const Duration(seconds: 300));
      await client.subscribe('d/t');
      await client.disconnect(properties: const [SessionExpiryInterval(0)]);
      await client.connect(
          cleanStart: false,
          sessionExpiryInterval: const Duration(seconds: 300));
      expect(client.sessionPresent, isFalse);
      await client.close();
    });

    test('DISCONNECT may raise the Session Expiry set in CONNECT', () async {
      broker = await Mosquitto.start();
      final client = newClient(broker.port, clientId: 'c-draise');
      await client.connect(sessionExpiryInterval: const Duration(seconds: 1));
      await client.subscribe('d/t');
      await client.disconnect(properties: const [SessionExpiryInterval(300)]);
      await settle(2000);
      await client.connect(
          cleanStart: false,
          sessionExpiryInterval: const Duration(seconds: 300));
      expect(client.sessionPresent, isTrue);
      await client.close();
    });

    test('CONNECT optional properties are accepted by the broker', () async {
      broker = await Mosquitto.start();
      final client = newClient(broker.port, clientId: 'c-props');
      await client.connect(
        receiveMaximum: 10,
        maximumPacketSize: 100000,
        topicAliasMaximum: 5,
        properties: const [
          RequestProblemInformation(1),
          RequestResponseInformation(1),
          UserProperty('app', 'test'),
          UserProperty('app', 'test2'),
        ],
      );
      expect(client.state, MqttConnectionState.connected);
      await client.close();
    });

    test('maximum keep alive 65535 is encodable', () async {
      broker = await Mosquitto.start();
      final client = newClient(broker.port, clientId: 'c-kamax');
      await client.connect(keepAlive: const Duration(seconds: 65535));
      await broker.waitForLog('as c-kamax (p5, c1, k65535)');
      await client.close();
    });
  });
}
