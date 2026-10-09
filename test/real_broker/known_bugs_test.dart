/// Reproductions of the defects found in review (BUG-1 ... BUG-11), each
/// asserting the behaviour the MQTT 5.0 specification (or basic reliability)
/// requires. They failed on 0.4.0 and pass since the fixes; kept as
/// regression tests. Every test runs against a real mosquitto broker; the
/// fault proxy only drops or cuts traffic.
@Tags(['real-broker'])
@Timeout(Duration(seconds: 60))
library;

import 'dart:async';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'auth_test.dart' show ProofAuthenticator, RefusingAuthenticator;
import 'support/common.dart';

const _sei = Duration(seconds: 300);

void main() {
  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
    return;
  }

  late Mosquitto broker;
  late FaultProxy proxy;
  tearDown(() async {
    await proxy.close();
    await broker.dispose();
  });

  test(
      'BUG-1 [MQTT-4.4.0-1] disconnect() forgets a QoS 2 exchange awaiting '
      'PUBCOMP; resuming the session never re-sends PUBREL, so the message '
      'stays stuck in the broker', () async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
    final witness = await MosqSub.start(broker.port, ['b1/t'], qos: 2);
    final c = newClient(proxy.port, clientId: 'b1');
    await c.connect(sessionExpiryInterval: _sei);
    proxy.dropWhen = (f) => f.dir == Dir.c2s && f.type == kPubrel;
    final a = c.publish('b1/t', bytes('A'), qos: MqttQos.exactlyOnce);
    a.ignore();
    await proxy.next((f) => f.dropped && f.type == kPubrel);
    await c.disconnect();
    proxy.dropWhen = null;

    await c.connect(cleanStart: false, sessionExpiryInterval: _sei);
    expect(c.sessionPresent, isTrue);
    await settle(1000);
    expect(proxy.sent(kPubrel).where((f) => f.connection == 2), isNotEmpty,
        reason: 'the resumed session owes the broker a PUBREL');
    expect(witness.messages.map((m) => m['payload']), ['A']);
    await witness.stop();
    await c.close();
  });

  test(
      'BUG-3 publishes waiting for a Receive Maximum slot are failed by a '
      'transient disconnect even though the session survives', () async {
    broker = await Mosquitto.start(config: ['max_inflight_messages 3']);
    proxy = await FaultProxy.start(broker.port);
    final c = newClient(proxy.port, clientId: 'b3');
    await c.connect(sessionExpiryInterval: _sei);
    proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
    final futures = [
      for (var i = 0; i < 10; i++)
        c.publish('b3/t', bytes('$i'), qos: MqttQos.atLeastOnce),
    ];
    await waitUntil(() => proxy.sent(kPublish).length == 3);
    proxy.dropWhen = null;
    proxy.cutAll();
    final outcomes = await Future.wait(futures.map(
        (f) => f.then((r) => 'ok', onError: (Object e) => '${e.runtimeType}')));
    expect(outcomes, List.filled(10, 'ok'));
    await c.close();
  });

  test(
      'BUG-4 a lost connection during re-authentication stops the client for '
      'good although autoReconnect is on', () async {
    final so = await buildAuthPlugin();
    broker = await Mosquitto.start(config: ['plugin $so']);
    proxy = await FaultProxy.start(broker.port);
    final c = newClient(proxy.port,
        clientId: 'b4', authenticator: ProofAuthenticator());
    await c.connect(
        authenticationMethod: 'TEST-CR',
        authenticationData: bytes('client-first'));
    proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kAuth;
    final reauth =
        c.reauthenticate(authenticationData: bytes('client-first-reauth'));
    reauth.ignore();
    await proxy.next((f) => f.dropped && f.type == kAuth);
    proxy.dropWhen = null;
    proxy.cutAll();
    await waitUntil(
        () =>
            proxy.connections >= 2 && c.state == MqttConnectionState.connected,
        timeout: const Duration(seconds: 5),
        reason: 'state=${c.state}, connections=${proxy.connections}');
    await c.close();
  });

  test(
      'BUG-5 a publish-only client never notices a dead link: outbound '
      'traffic resets keep alive, so PINGREQ is never sent', () async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
    final c = newClient(proxy.port,
        clientId: 'b5', pingResponseTimeout: const Duration(seconds: 1));
    await c.connect(keepAlive: const Duration(seconds: 1));
    proxy.blackhole = true;
    var stop = false;
    final telemetry = () async {
      while (!stop) {
        try {
          await c.publish('b5/t', bytes('reading'));
        } on MqttException {
          // Not connected: that is exactly what we want to see.
        }
        await Future<void>.delayed(const Duration(milliseconds: 200));
      }
    }();
    await waitUntil(() => c.state != MqttConnectionState.connected,
        timeout: const Duration(seconds: 5),
        reason: 'still "connected" after 5 s of a black-holed link; '
            'PINGREQs sent: ${proxy.sent(kPingreq).length}');
    stop = true;
    await telemetry;
    await c.close();
  });

  test(
      'BUG-6 [MQTT-1.5.4-3] a leading U+FEFF (BOM) in a received topic is '
      'stripped', () async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
    final c = newClient(proxy.port, clientId: 'b6');
    await c.connect();
    final inbox = Inbox(c);
    await c.subscribe('#');
    await mosqPub(broker.port, '﻿bom/t', 'x');
    await inbox.waitFor(1);
    final wireTopic = proxy.received(kPublish).single.publishTopicBytes;
    expect(wireTopic.sublist(0, 3), [0xEF, 0xBB, 0xBF],
        reason: 'the broker forwarded the BOM');
    expect(inbox.messages.single.topic, '﻿bom/t');
    await c.close();
  });

  test(
      'BUG-7 [MQTT-3.3.4-6] a Subscription Identifier in an outgoing PUBLISH '
      'is not rejected locally; the broker drops the connection', () async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
    final c = newClient(proxy.port, clientId: 'b7');
    await c.connect();
    Object? error;
    try {
      await c.publish('b7/t', bytes('x'),
          qos: MqttQos.atLeastOnce,
          properties: const [SubscriptionIdentifier(1)]);
    } on Object catch (e) {
      error = e;
    }
    await settle(500);
    printOnFailure(proxy.dump());
    expect(error, isA<ArgumentError>());
    expect(proxy.sent(kPublish), isEmpty);
    expect(proxy.connections, 1);
    await c.close();
  });

  test(
      'BUG-8 Session taken over (0x8E) is treated as retryable: two clients '
      'with the same id evict each other in an endless loop', () async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
    final a = newClient(broker.port, clientId: 'dup');
    final b = newClient(broker.port, clientId: 'dup');
    await a.connect();
    await b.connect();
    await settle(3000);
    final takeovers = broker.countLog('session taken over');
    await a.close();
    await b.close();
    expect(takeovers, lessThanOrEqualTo(1),
        reason: '$takeovers takeovers in 3 s');
  });

  test('BUG-9 [Table 3-10] client sends server-only DISCONNECT reason codes',
      () async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
    final c = newClient(proxy.port, clientId: 'b9');
    await c.connect();
    Object? error;
    try {
      await c.disconnect(reasonCode: MqttReasonCode.sessionTakenOver);
    } on Object catch (e) {
      error = e;
    }
    await settle(300);
    final sent = proxy.sent(kDisconnect).map((f) => f.ackReasonCode).toList();
    printOnFailure('error=$error DISCONNECT reason codes on the wire: $sent');
    expect(error is ArgumentError || !sent.contains(0x8E), isTrue);
    await c.close();
  });

  test(
      'BUG-9b the library itself sends DISCONNECT 0x87 (server-only) when the '
      'authenticator aborts', () async {
    final so = await buildAuthPlugin();
    broker = await Mosquitto.start(config: ['plugin $so']);
    proxy = await FaultProxy.start(broker.port);
    final c = newClient(proxy.port,
        clientId: 'b9b', authenticator: RefusingAuthenticator());
    await expectLater(
        c.connect(
            authenticationMethod: 'TEST-CR',
            authenticationData: bytes('client-first')),
        throwsA(isA<MqttAuthenticationException>()));
    await settle(300);
    final codes = proxy.sent(kDisconnect).map((f) => f.ackReasonCode).toList();
    printOnFailure('client DISCONNECT reason codes: $codes');
    expect(codes, isNot(contains(0x87)));
    await c.close();
  });

  test(
      'BUG-10 QoS 1/2 messages that arrive before the application listens '
      '(e.g. a resumed session\'s backlog right after connect()) are '
      'acknowledged to the broker and then dropped', () async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
    final c = newClient(proxy.port, clientId: 'b10');
    await c.connect(sessionExpiryInterval: _sei);
    await c.subscribe('b10/t',
        options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
    await c.disconnect();
    for (var i = 0; i < 20; i++) {
      await mosqPub(broker.port, 'b10/t', 'm$i', qos: 1);
    }
    await c.connect(cleanStart: false, sessionExpiryInterval: _sei);
    final inbox = Inbox(c);
    await settle(1500);
    final acked = proxy.sent(kPuback).length;
    expect(acked, 20, reason: 'the broker was told all 20 were received');
    expect(inbox.messages, hasLength(acked),
        reason: '$acked acknowledged, ${inbox.messages.length} delivered');
    await c.close();
  });

  test(
      'BUG-11 [MQTT-1.5.4-1] a topic with a lone surrogate is silently '
      'rewritten to U+FFFD and published to a different topic', () async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
    final c = newClient(proxy.port, clientId: 'b11');
    await c.connect();
    Object? error;
    try {
      await c.publish('b11/\uD800x', bytes('x'), qos: MqttQos.atLeastOnce);
    } on Object catch (e) {
      error = e;
    }
    await settle(200);
    final wire = proxy.sent(kPublish).map((f) => f.publish.topic).toList();
    printOnFailure('topics on the wire: $wire');
    expect(error, isA<ArgumentError>());
    expect(wire, isEmpty);
    await c.close();
  });
}
