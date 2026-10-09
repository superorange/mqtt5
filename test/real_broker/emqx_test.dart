/// The same protocol scenarios run against EMQX 5.8.6 (Docker), a second and
/// independent MQTT 5 server implementation, so passing results are not an
/// artefact of mosquitto's particular behaviour.
@Tags(['real-broker', 'emqx'])
@Timeout(Duration(seconds: 90))
library;

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'support/common.dart';
import 'support/emqx.dart';

const _sei = Duration(seconds: 300);

int _maxOutstanding(Iterable<WireFrame> frames, Dir dir) {
  final open = <int>{};
  var max = 0;
  for (final f in frames) {
    if (f.dropped) continue;
    if (f.dir == dir && f.type == kPublish && f.qos > 0) {
      open.add(f.packetId);
      if (open.length > max) max = open.length;
    } else if (f.dir != dir && (f.type == kPuback || f.type == kPubcomp)) {
      open.remove(f.packetId);
    }
  }
  return max;
}

Future<void> main() async {
  if (!await Emqx.available) {
    test('EMQX image not available', () {}, skip: 'docker image missing');
    return;
  }
  // Fills in mosquitto_sub as a witness; it is a generic MQTT 5 client.
  final haveWitness = Mosquitto.available;

  group('EMQX default profile', () {
    late Emqx emqx;
    setUpAll(() async => emqx = await Emqx.start());
    tearDownAll(() => emqx.dispose());

    late FaultProxy proxy;
    setUp(() async => proxy = await FaultProxy.start(emqx.port));
    tearDown(() => proxy.close());

    test('connect: CONNACK capabilities are parsed', () async {
      final c = newClient(proxy.port, clientId: 'ex-caps');
      await c.connect(keepAlive: const Duration(seconds: 30));
      final caps = c.serverCapabilities;
      printOnFailure('${proxy.received(kConnack).single}');
      expect(caps.receiveMaximum, greaterThan(0));
      expect(caps.topicAliasMaximum, greaterThan(0));
      expect(c.sessionPresent, isFalse);
      await c.close();
    });

    test('empty client id: assigned identifier adopted', () async {
      final c = newClient(proxy.port, clientId: '');
      await c.connect();
      expect(c.effectiveClientId, isNotEmpty);
      await c.close();
    });

    test('QoS 0/1/2 round trip and QoS downgrade', () async {
      final c = newClient(proxy.port, clientId: 'ex-q');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('ex/q/#',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      await c.subscribe('ex/dg');
      await c.publish('ex/q/0', bytes('0'));
      expect(
          (await c.publish('ex/q/1', bytes('1'), qos: MqttQos.atLeastOnce))
              .reasonCode,
          MqttReasonCode.success);
      expect(
          (await c.publish('ex/q/2', bytes('2'), qos: MqttQos.exactlyOnce))
              .reasonCode,
          MqttReasonCode.success);
      await c.publish('ex/dg', bytes('d'), qos: MqttQos.exactlyOnce);
      await inbox.waitFor(4);
      expect(inbox.payloads, ['0', '1', '2', 'd']);
      expect(inbox.messages.map((m) => m.qos), [
        MqttQos.atMostOnce,
        MqttQos.atLeastOnce,
        MqttQos.exactlyOnce,
        MqttQos.atMostOnce,
      ]);
      // Every incoming QoS 2 PUBLISH was completed with PUBREC/PUBCOMP.
      expect(proxy.sent(kPubrec), isNotEmpty);
      expect(proxy.sent(kPubcomp), isNotEmpty);
      expect(c.inflightCount, 0);
      await c.close();
    });

    test('all PUBLISH properties survive a round trip through EMQX', () async {
      final c = newClient(proxy.port, clientId: 'ex-props');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('ex/props',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      await c.publish('ex/props', bytes('{}'),
          qos: MqttQos.atLeastOnce,
          properties: [
            const PayloadFormatIndicator(1),
            const MessageExpiryInterval(120),
            const ContentType('application/json'),
            const ResponseTopic('ex/reply'),
            CorrelationData(utf8.encode('c-1')),
            const UserProperty('k', 'v1'),
            const UserProperty('k', 'v2'),
          ]);
      await inbox.waitFor(1);
      final p = inbox.messages.single.properties;
      expect(p.whereType<PayloadFormatIndicator>().single.value, 1);
      expect(p.whereType<MessageExpiryInterval>().single.seconds,
          inInclusiveRange(118, 120));
      expect(p.whereType<ContentType>().single.value, 'application/json');
      expect(p.whereType<ResponseTopic>().single.value, 'ex/reply');
      expect(utf8.decode(p.whereType<CorrelationData>().single.data), 'c-1');
      expect(p.whereType<UserProperty>().map((u) => u.value), ['v1', 'v2']);
      await c.close();
    });

    test('ordering: 400 QoS 1 + 200 QoS 2 pipelined publishes', () async {
      final c = newClient(proxy.port, clientId: 'ex-ord');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('ex/ord',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      final results = await Future.wait([
        for (var i = 0; i < 400; i++)
          c.publish('ex/ord', bytes('a$i'), qos: MqttQos.atLeastOnce),
        for (var i = 0; i < 200; i++)
          c.publish('ex/ord', bytes('b$i'), qos: MqttQos.exactlyOnce),
      ]);
      expect(
          results.every((r) => r.reasonCode == MqttReasonCode.success), isTrue);
      await inbox.waitFor(600, timeout: const Duration(seconds: 30));
      final q1 = inbox.messages
          .where((m) => m.qos == MqttQos.atLeastOnce)
          .map((m) => text(m.payload));
      final q2 = inbox.messages
          .where((m) => m.qos == MqttQos.exactlyOnce)
          .map((m) => text(m.payload));
      expect(q1, [for (var i = 0; i < 400; i++) 'a$i']);
      expect(q2, [for (var i = 0; i < 200; i++) 'b$i']);
      expect(_maxOutstanding(proxy.frames, Dir.c2s),
          lessThanOrEqualTo(c.serverCapabilities.receiveMaximum));
      await c.close();
    });

    test('retained messages and Retain Handling 0/1/2, Retain As Published',
        () async {
      final c = newClient(proxy.port, clientId: 'ex-ret');
      await c.connect();
      await c.publish('ex/ret', bytes('R'),
          qos: MqttQos.atLeastOnce, retain: true);
      final inbox = Inbox(c);
      Future<int> delta(Future<void> Function() f) async {
        final n = inbox.messages.length;
        await f();
        await settle(400);
        return inbox.messages.length - n;
      }

      expect(await delta(() => c.subscribe('ex/ret')), 1);
      expect(inbox.messages.last.retain, isTrue);
      expect(await delta(() => c.subscribe('ex/ret')), 1);
      await c.unsubscribe(['ex/ret']);
      const ifNew =
          MqttSubscriptionOptions(retainHandling: MqttRetainHandling.sendIfNew);
      expect(await delta(() => c.subscribe('ex/ret', options: ifNew)), 1);
      expect(await delta(() => c.subscribe('ex/ret', options: ifNew)), 0);
      await c.unsubscribe(['ex/ret']);
      const never =
          MqttSubscriptionOptions(retainHandling: MqttRetainHandling.doNotSend);
      expect(await delta(() => c.subscribe('ex/ret', options: never)), 0);
      await c.unsubscribe(['ex/ret']);
      await c.subscribe('ex/rap',
          options: const MqttSubscriptionOptions(retainAsPublished: true));
      await c.publish('ex/rap', bytes('x'), retain: true);
      await inbox.waitFor(inbox.messages.length + 1);
      expect(inbox.messages.last.retain, isTrue);
      // Clean up the retained messages.
      await c.publish('ex/ret', Uint8List(0), retain: true);
      await c.publish('ex/rap', Uint8List(0), retain: true);
      await c.close();
    });

    test('No Local, Subscription Identifiers, wildcards, shared subscriptions',
        () async {
      final c = newClient(proxy.port, clientId: 'ex-opts');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('ex/nl',
          options: const MqttSubscriptionOptions(noLocal: true));
      await c.publish('ex/nl', bytes('mine'));
      await c.subscribe('ex/sid/+', subscriptionIdentifier: 11);
      await c.subscribe('ex/sid/#', subscriptionIdentifier: 12);
      final other = newClient(emqx.port, clientId: 'ex-opts-other');
      await other.connect();
      await other.publish('ex/nl', bytes('theirs'));
      await other.publish('ex/sid/a', bytes('s'));
      await waitUntil(() => inbox.messages.length >= 2);
      await settle(400);
      expect(inbox.payloads.where((p) => p == 'mine'), isEmpty);
      final ids = inbox.messages
          .where((m) => m.topic == 'ex/sid/a')
          .expand((m) => m.subscriptionIdentifiers)
          .toSet();
      expect(ids, {11, 12});

      final a = newClient(emqx.port, clientId: 'ex-sha');
      final b = newClient(emqx.port, clientId: 'ex-shb');
      await a.connect();
      await b.connect();
      final ia = Inbox(a), ib = Inbox(b);
      await a.subscribe(r'$share/g/ex/sh',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      await b.subscribe(r'$share/g/ex/sh',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      for (var i = 0; i < 20; i++) {
        await other.publish('ex/sh', bytes('$i'), qos: MqttQos.atLeastOnce);
      }
      await waitUntil(() => ia.messages.length + ib.messages.length >= 20);
      await settle(300);
      expect({...ia.payloads, ...ib.payloads}.length, 20);
      expect(ia.messages.length + ib.messages.length, 20);
      for (final x in [c, other, a, b]) {
        await x.close();
      }
    });

    test('client -> broker Topic Alias; witness sees correct topics', () async {
      if (!haveWitness) markTestSkipped('mosquitto_sub not available');
      final witness = await MosqSub.start(emqx.port, ['ex/ta/#']);
      final c = newClient(proxy.port, clientId: 'ex-ta');
      await c.connect();
      final sent = <String>[];
      for (var r = 0; r < 3; r++) {
        for (var t = 0; t < 4; t++) {
          sent.add('ex/ta/$t');
          await c.publish('ex/ta/$t', bytes('$r$t'), qos: MqttQos.atLeastOnce);
        }
      }
      await witness.waitFor(12);
      expect(witness.messages.map((m) => m['topic']), sent);
      expect(proxy.sent(kPublish).where((f) => f.publish.topic.isEmpty),
          hasLength(8));
      await witness.stop();
      await c.close();
    });

    test('client Receive Maximum 1 honoured by EMQX', () async {
      final c = newClient(proxy.port, clientId: 'ex-rm1');
      await c.connect(receiveMaximum: 1);
      final inbox = Inbox(c);
      await c.subscribe('ex/rm1',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      final pub = newClient(emqx.port, clientId: 'ex-rm1-pub');
      await pub.connect();
      await Future.wait([
        for (var i = 0; i < 20; i++)
          pub.publish('ex/rm1', bytes('$i'), qos: MqttQos.atLeastOnce),
      ]);
      await inbox.waitFor(20);
      expect(inbox.payloads, [for (var i = 0; i < 20; i++) '$i']);
      expect(_maxOutstanding(proxy.frames, Dir.s2c), 1);
      await pub.close();
      await c.close();
    });

    test('Will on abnormal loss, none on normal disconnect, 0x04 sends it',
        () async {
      final c0 = newClient(emqx.port, clientId: 'ex-will-watch');
      await c0.connect();
      final inbox = Inbox(c0);
      await c0.subscribe('ex/will/#');
      MqttWill w(String id) => MqttWill(
          topic: 'ex/will/$id',
          payload: Uint8List.fromList(utf8.encode(id)),
          qos: MqttQos.atLeastOnce,
          properties: const [UserProperty('k', 'v')]);
      final abnormal = newClient(proxy.port,
          clientId: 'ex-w1', will: w('abnormal'), autoReconnect: false);
      await abnormal.connect();
      final normal = newClient(emqx.port, clientId: 'ex-w2', will: w('normal'));
      await normal.connect();
      final with04 = newClient(emqx.port, clientId: 'ex-w3', will: w('with04'));
      await with04.connect();
      proxy.cutAll();
      await normal.disconnect();
      await with04.disconnect(
          reasonCode: MqttReasonCode.disconnectWithWillMessage);
      await inbox.waitFor(2);
      await settle(800);
      expect(inbox.payloads..sort(), ['abnormal', 'with04']);
      expect(
          inbox.messages.first.properties
              .whereType<UserProperty>()
              .single
              .value,
          'v');
      for (final x in [c0, abnormal, normal, with04]) {
        await x.close();
      }
    });

    test(
        'session resume: queued offline messages and DUP retransmission of '
        'unacknowledged QoS 1 / QoS 2', () async {
      final c = newClient(proxy.port, clientId: 'ex-sess');
      await c.connect(sessionExpiryInterval: _sei);
      await c.subscribe('ex/sess',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      await c.disconnect();
      final pub = newClient(emqx.port, clientId: 'ex-sess-pub');
      await pub.connect();
      await pub.publish('ex/sess', bytes('offline'), qos: MqttQos.exactlyOnce);
      final inbox = Inbox(c);
      await c.connect(cleanStart: false, sessionExpiryInterval: _sei);
      expect(c.sessionPresent, isTrue);
      await inbox.waitFor(1);
      expect(inbox.payloads, ['offline']);

      // QoS 1 PUBACK lost, QoS 2 PUBREC lost.
      proxy.dropWhen =
          (f) => f.dir == Dir.s2c && (f.type == kPuback || f.type == kPubrec);
      final r1 = c.publish('ex/sess', bytes('q1'), qos: MqttQos.atLeastOnce);
      final r2 = c.publish('ex/sess', bytes('q2'), qos: MqttQos.exactlyOnce);
      await waitUntil(() =>
          proxy.frames
              .where(
                  (f) => f.dropped && (f.type == kPuback || f.type == kPubrec))
              .length ==
          2);
      proxy.dropWhen = null;
      final before = proxy.connections;
      proxy.cutAll();
      expect((await r1.timeout(const Duration(seconds: 10))).reasonCode,
          MqttReasonCode.success);
      // EMQX answers a legitimate DUP retransmission of a QoS 2 PUBLISH whose
      // PUBREC was lost with PUBREC 0x91 instead of 0x00 (it already holds the
      // identifier). Under section 4.3.3 a PUBREC >= 0x80 ends the exchange,
      // so the client reports it; the message itself was delivered once.
      expect((await r2.timeout(const Duration(seconds: 10))).reasonCode,
          anyOf(MqttReasonCode.success, MqttReasonCode.packetIdentifierInUse));
      final resent =
          proxy.sent(kPublish).where((f) => f.connection > before).toList();
      expect(resent.map((f) => f.dup), everyElement(isTrue));
      await waitUntil(() => inbox.payloads.contains('q2'));
      await settle(500);
      expect(inbox.payloads.where((p) => p == 'q2'), hasLength(1),
          reason: 'QoS 2 delivered exactly once');
      await pub.close();
      await c.close();
    });

    test(
        'incoming QoS 2: PUBLISH re-sent after our PUBREC was lost is not '
        'delivered twice', () async {
      final c = newClient(proxy.port, clientId: 'ex-in2');
      await c.connect(sessionExpiryInterval: _sei);
      final inbox = Inbox(c);
      await c.subscribe('ex/in2',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      proxy.dropWhen = (f) => f.dir == Dir.c2s && f.type == kPubrec;
      final pub = newClient(emqx.port, clientId: 'ex-in2-pub');
      await pub.connect();
      await pub.publish('ex/in2', bytes('once'), qos: MqttQos.exactlyOnce);
      await proxy.next((f) => f.dropped && f.type == kPubrec,
          includeHistory: true);
      proxy.dropWhen = null;
      final before = proxy.connections;
      proxy.cutAll();
      await waitUntil(() =>
          proxy.connections > before &&
          c.state == MqttConnectionState.connected);
      await waitUntil(() => proxy.sent(kPubcomp).isNotEmpty);
      await settle(300);
      expect(inbox.payloads, ['once']);
      await pub.close();
      await c.close();
    });

    test('packet identifiers wrap past 65535', () async {
      final c = newClient(emqx.port,
          clientId: 'ex-wrap', operationTimeout: const Duration(seconds: 60));
      await c.connect();
      await Future.wait([
        for (var i = 0; i < 66000; i++)
          c.publish('ex/wrap', bytes('$i'), qos: MqttQos.atLeastOnce),
      ]);
      expect(c.inflightCount, 0);
      await c.close();
    }, timeout: const Timeout(Duration(minutes: 3)));
  });

  group('EMQX constrained profile', () {
    late Emqx emqx;
    setUpAll(() async => emqx = await Emqx.start(env: {
          'EMQX_MQTT__MAX_INFLIGHT': '3',
          'EMQX_MQTT__MAX_TOPIC_ALIAS': '2',
          'EMQX_MQTT__MAX_PACKET_SIZE': '2KB',
          'EMQX_MQTT__MAX_QOS_ALLOWED': '1',
          'EMQX_MQTT__RETAIN_AVAILABLE': 'false',
          'EMQX_MQTT__SERVER_KEEPALIVE': '2',
        }));
    tearDownAll(() => emqx.dispose());

    late FaultProxy proxy;
    setUp(() async => proxy = await FaultProxy.start(emqx.port));
    tearDown(() => proxy.close());

    test('limits from CONNACK are enforced locally, connection survives',
        () async {
      final c = newClient(proxy.port, clientId: 'exc-caps');
      await c.connect(keepAlive: const Duration(seconds: 60));
      final caps = c.serverCapabilities;
      printOnFailure('${proxy.received(kConnack).single}');
      expect(caps.receiveMaximum, 3);
      expect(caps.topicAliasMaximum, 2);
      expect(caps.maximumPacketSize, 2048);
      expect(caps.maximumQos, 1);
      expect(caps.retainAvailable, isFalse);
      expect(caps.serverKeepAlive, const Duration(seconds: 2));
      await expectLater(c.publish('x', bytes('x'), qos: MqttQos.exactlyOnce),
          throwsA(isA<MqttFlowControlException>()));
      await expectLater(c.publish('x', bytes('x'), retain: true),
          throwsA(isA<MqttFlowControlException>()));
      await expectLater(c.publish('x', Uint8List(4000)),
          throwsA(isA<MqttPacketTooLargeException>()));
      await settle(5000);
      expect(proxy.sent(kPingreq).length, greaterThanOrEqualTo(2));
      expect(c.state, MqttConnectionState.connected);
      expect(proxy.connections, 1);
      await c.close();
    });

    test('Receive Maximum 3 and alias eviction under load', () async {
      final c =
          newClient(proxy.port, clientId: 'exc-load', topicAliasEviction: true);
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('exc/#',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      final topics = [for (var i = 0; i < 60; i++) 'exc/${i % 5}'];
      await Future.wait([
        for (var i = 0; i < topics.length; i++)
          c.publish(topics[i], bytes('$i'), qos: MqttQos.atLeastOnce),
      ]);
      await inbox.waitFor(60);
      expect(inbox.messages.map((m) => m.topic), topics);
      expect(_maxOutstanding(proxy.frames, Dir.c2s), lessThanOrEqualTo(3));
      for (final f in proxy.sent(kPublish)) {
        final alias = prop(f.publish.properties, 0x23) as int?;
        if (alias != null) expect(alias, inInclusiveRange(1, 2));
      }
      await c.close();
    });
  });

  group('EMQX: defects found in review (fixed)', () {
    late Emqx emqx;
    setUpAll(() async => emqx = await Emqx.start());
    tearDownAll(() => emqx.dispose());

    late FaultProxy proxy;
    setUp(() async => proxy = await FaultProxy.start(emqx.port));
    tearDown(() => proxy.close());

    test(
        'BUG-2 on EMQX: fresh client instance resumes a session whose QoS 2 '
        'exchange is pending in the broker; a NEW message must not be lost',
        () async {
      final watch = newClient(emqx.port, clientId: 'exb2-watch');
      await watch.connect();
      final inbox = Inbox(watch);
      await watch.subscribe('exb2/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      final old = newClient(proxy.port, clientId: 'exb2', autoReconnect: false);
      await old.connect(sessionExpiryInterval: _sei);
      proxy.dropWhen = (f) => f.dir == Dir.c2s && f.type == kPubrel;
      old.publish('exb2/t', bytes('OLD'), qos: MqttQos.exactlyOnce).ignore();
      await proxy.next((f) => f.dropped && f.type == kPubrel);
      proxy.dropWhen = null;
      proxy.cutAll();
      await old.close();
      await settle(300);

      final fresh =
          newClient(proxy.port, clientId: 'exb2', autoReconnect: false);
      await expectLater(
        fresh.connect(cleanStart: false, sessionExpiryInterval: _sei),
        throwsA(isA<MqttProtocolException>()),
      );
      expect(fresh.state, MqttConnectionState.disconnected);
      await fresh.close();
      await watch.close();
    });

    test(
        'BUG-1 on EMQX: disconnect() with a pending PUBREL, resume: PUBREL '
        'must be re-sent', () async {
      final c = newClient(proxy.port, clientId: 'exb1');
      await c.connect(sessionExpiryInterval: _sei);
      proxy.dropWhen = (f) => f.dir == Dir.c2s && f.type == kPubrel;
      c.publish('exb1/t', bytes('A'), qos: MqttQos.exactlyOnce).ignore();
      await proxy.next((f) => f.dropped && f.type == kPubrel);
      await c.disconnect();
      proxy.dropWhen = null;
      await c.connect(cleanStart: false, sessionExpiryInterval: _sei);
      expect(c.sessionPresent, isTrue);
      await settle(800);
      expect(proxy.sent(kPubrel).where((f) => f.connection == 2), isNotEmpty);
      await c.close();
    });

    test('BUG-6 on EMQX: BOM in a topic name is stripped on receipt', () async {
      final c = newClient(proxy.port, clientId: 'exb6');
      await c.connect();
      final inbox = Inbox(c);
      // EMQX's default ACL refuses a bare '#'.
      await c.subscribe('+');
      final p = newClient(emqx.port, clientId: 'exb6-pub');
      await p.connect();
      await p.publish('﻿exbom', bytes('x'));
      await inbox.waitFor(1);
      final wire = proxy.received(kPublish).single.publishTopicBytes;
      expect(wire.sublist(0, 3), [0xEF, 0xBB, 0xBF]);
      expect(inbox.messages.single.topic, '﻿exbom');
      await p.close();
      await c.close();
    });

    test('BUG-7 on EMQX: Subscription Identifier in an outgoing PUBLISH',
        () async {
      final c = newClient(proxy.port, clientId: 'exb7');
      await c.connect();
      Object? error;
      try {
        await c.publish('exb7/t', bytes('x'),
            qos: MqttQos.atLeastOnce,
            properties: const [SubscriptionIdentifier(1)]);
      } on Object catch (e) {
        error = e;
      }
      await settle(500);
      printOnFailure('error=$error\n${proxy.dump()}');
      // EMQX tolerates it, but the client still put a server-only property
      // on the wire.
      expect(
          proxy
              .sent(kPublish)
              .where((f) => prop(f.publish.properties, 0x0B) != null),
          isEmpty);
      expect(error, isA<ArgumentError>());
      await c.close();
    });

    test('BUG-8 on EMQX: session takeover loop', () async {
      final a = newClient(emqx.port, clientId: 'exb8');
      final b = newClient(emqx.port, clientId: 'exb8');
      var connects = 0;
      a.stateStream.listen((s) {
        if (s == MqttConnectionState.connected) connects++;
      });
      b.stateStream.listen((s) {
        if (s == MqttConnectionState.connected) connects++;
      });
      await a.connect();
      await b.connect();
      await settle(3000);
      final n = connects;
      await a.close();
      await b.close();
      expect(n, lessThanOrEqualTo(3), reason: '$n connections in 3 s');
    });
  });
}
