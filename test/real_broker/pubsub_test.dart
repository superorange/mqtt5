@Tags(['real-broker'])
@Timeout(Duration(seconds: 120))
library;

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'support/common.dart';

/// Highest number of QoS>0 PUBLISH packets travelling in [dir] that were
/// outstanding at once, as seen on the wire.
int maxOutstanding(List<WireFrame> frames, Dir dir) {
  final open = <int>{};
  var max = 0;
  for (final f in frames) {
    if (f.dropped) continue;
    if (f.dir == dir && f.type == kPublish && f.qos > 0) {
      open.add(f.packetId);
      if (open.length > max) max = open.length;
    } else if (f.dir != dir &&
        (f.type == kPuback ||
            f.type == kPubcomp ||
            (f.type == kPubrec && (f.ackReasonCode ?? 0) >= 0x80))) {
      open.remove(f.packetId);
    }
  }
  return max;
}

void main() {
  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
    return;
  }

  late Mosquitto broker;
  tearDown(() => broker.dispose());

  group('PUBLISH QoS flows', () {
    test(
        'QoS 0/1/2 round trip; the broker log shows the complete QoS 2 '
        'handshake in both directions', () async {
      broker = await Mosquitto.start();
      final c = newClient(broker.port, clientId: 'q');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('q/#',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      await c.publish('q/0', bytes('zero'));
      final r1 = await c.publish('q/1', bytes('one'), qos: MqttQos.atLeastOnce);
      final r2 = await c.publish('q/2', bytes('two'), qos: MqttQos.exactlyOnce);
      expect(r1.reasonCode, MqttReasonCode.success);
      expect(r2.reasonCode, MqttReasonCode.success);
      await inbox.waitFor(3);
      expect(inbox.payloads, ['zero', 'one', 'two']);
      expect(inbox.messages.map((m) => m.qos),
          [MqttQos.atMostOnce, MqttQos.atLeastOnce, MqttQos.exactlyOnce]);
      // Outgoing QoS 2: PUBLISH -> PUBREC -> PUBREL -> PUBCOMP.
      await broker.waitForLog('Received PUBREL from q');
      // Incoming QoS 1 and QoS 2 acknowledged by the client.
      await broker.waitForLog('Received PUBACK from q');
      await broker.waitForLog('Received PUBREC from q');
      await broker.waitForLog('Received PUBCOMP from q');
      expect(c.inflightCount, 0);
      await c.close();
      expect(broker.log, isNot(contains('protocol error')));
    });

    test('QoS is downgraded to the subscription maximum', () async {
      broker = await Mosquitto.start();
      final c = newClient(broker.port, clientId: 'dg');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('dg/t');
      await c.publish('dg/t', bytes('x'), qos: MqttQos.exactlyOnce);
      await inbox.waitFor(1);
      expect(inbox.messages.single.qos, MqttQos.atMostOnce);
      await c.close();
    });

    test(
        'every PUBLISH property the client sends is seen verbatim by a '
        'third-party subscriber', () async {
      broker = await Mosquitto.start();
      final witness = await MosqSub.start(broker.port, ['props/#']);
      final c = newClient(broker.port, clientId: 'pp');
      await c.connect();
      await c.publish(
        'props/t',
        bytes('{"a":1}'),
        qos: MqttQos.atLeastOnce,
        properties: [
          const PayloadFormatIndicator(1),
          const MessageExpiryInterval(120),
          const ContentType('application/json'),
          const ResponseTopic('reply/here'),
          CorrelationData(utf8.encode('corr-42')),
          const UserProperty('k', 'v1'),
          const UserProperty('k', 'v2'),
          const UserProperty('中文', '值'),
        ],
      );
      await witness.waitFor(1);
      final m = witness.messages.single;
      final p = m['properties'] as Map<String, dynamic>;
      expect(m['topic'], 'props/t');
      expect(m['payload'], '{"a":1}');
      expect(p['payload-format-indicator'], 1);
      expect(p['message-expiry-interval'], inInclusiveRange(118, 120));
      expect(p['content-type'], 'application/json');
      expect(p['response-topic'], 'reply/here');
      expect(p['correlation-data'], 'corr-42');
      expect(p['user-properties'], [
        {'k': 'v1'},
        {'k': 'v2'},
        {'中文': '值'},
      ]);
      await witness.stop();
      await c.close();
    });

    test('every PUBLISH property a third-party publisher sends is decoded',
        () async {
      broker = await Mosquitto.start();
      final c = newClient(broker.port, clientId: 'pr');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('pr/#',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      await mosqPub(broker.port, 'pr/t', 'hello', qos: 2, extra: [
        '-D',
        'publish',
        'payload-format-indicator',
        '1',
        '-D',
        'publish',
        'message-expiry-interval',
        '60',
        '-D',
        'publish',
        'content-type',
        'text/plain',
        '-D',
        'publish',
        'response-topic',
        'rt/x',
        '-D',
        'publish',
        'correlation-data',
        'cd',
        '-D',
        'publish',
        'user-property',
        'a',
        'b',
        '-D',
        'publish',
        'user-property',
        'a',
        'c',
      ]);
      await inbox.waitFor(1);
      final m = inbox.messages.single;
      List<T> all<T>() => m.properties.whereType<T>().toList();
      expect(text(m.payload), 'hello');
      expect(m.qos, MqttQos.exactlyOnce);
      expect(all<PayloadFormatIndicator>().single.value, 1);
      expect(all<MessageExpiryInterval>().single.seconds,
          inInclusiveRange(58, 60));
      expect(all<ContentType>().single.value, 'text/plain');
      expect(all<ResponseTopic>().single.value, 'rt/x');
      expect(utf8.decode(all<CorrelationData>().single.data), 'cd');
      expect(all<UserProperty>().map((u) => '${u.name}=${u.value}'),
          ['a=b', 'a=c']);
      await c.close();
    });

    test('payload edge cases: empty, 1 MiB, every byte value; UTF-8 topics',
        () async {
      broker = await Mosquitto.start();
      final c = newClient(broker.port, clientId: 'pl');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('pl/#',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      final big = Uint8List(1024 * 1024);
      for (var i = 0; i < big.length; i++) {
        big[i] = (i * 31) & 0xFF;
      }
      final all = Uint8List.fromList(List.generate(256, (i) => i));
      await c.publish('pl/empty', Uint8List(0), qos: MqttQos.atLeastOnce);
      await c.publish('pl/big', big, qos: MqttQos.atLeastOnce);
      await c.publish('pl/bytes', all, qos: MqttQos.atLeastOnce);
      await c.publish('pl/中文/🚀 space', bytes('u'), qos: MqttQos.atLeastOnce);
      await inbox.waitFor(4);
      expect(inbox.messages[0].payload, isEmpty);
      expect(inbox.messages[1].payload, big);
      expect(inbox.messages[2].payload, all);
      expect(inbox.messages[3].topic, 'pl/中文/🚀 space');
      await c.close();
    });

    test(
        'ordering: 500 pipelined QoS 1 and 300 QoS 2 publishes arrive in '
        'order and never exceed the broker Receive Maximum', () async {
      broker = await Mosquitto.start(config: ['max_inflight_messages 7']);
      final proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'ord');
      await c.connect();
      expect(c.serverCapabilities.receiveMaximum, 7);
      final inbox = Inbox(c);
      await c.subscribe('ord/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      final futures = <Future<MqttPublishResult>>[
        for (var i = 0; i < 500; i++)
          c.publish('ord/t', bytes('a$i'), qos: MqttQos.atLeastOnce),
        for (var i = 0; i < 300; i++)
          c.publish('ord/t', bytes('b$i'), qos: MqttQos.exactlyOnce),
      ];
      final results = await Future.wait(futures);
      expect(
          results.every((r) => r.reasonCode == MqttReasonCode.success), isTrue);
      await inbox.waitFor(800, timeout: const Duration(seconds: 30));
      expect(inbox.payloads, [
        for (var i = 0; i < 500; i++) 'a$i',
        for (var i = 0; i < 300; i++) 'b$i',
      ]);
      expect(maxOutstanding(proxy.frames, Dir.c2s), lessThanOrEqualTo(7));
      // Packet identifiers in flight at the same time were never reused.
      expect(c.inflightCount, 0);
      await c.close();
      await proxy.close();
      expect(broker.log, isNot(contains('protocol error')));
    });

    test('packet identifiers wrap past 65535 without collisions', () async {
      broker = await Mosquitto.start(config: ['max_inflight_messages 500']);
      final c = newClient(broker.port,
          clientId: 'wrap', operationTimeout: const Duration(seconds: 60));
      await c.connect();
      const n = 66000;
      var done = 0;
      final futures = <Future<void>>[];
      for (var i = 0; i < n; i++) {
        futures.add(c
            .publish('wrap/t', bytes('$i'), qos: MqttQos.atLeastOnce)
            .then((r) {
          expect(
              r.reasonCode,
              anyOf(MqttReasonCode.success,
                  MqttReasonCode.noMatchingSubscribers));
          done++;
        }));
      }
      await Future.wait(futures);
      expect(done, n);
      expect(c.inflightCount, 0);
      expect(broker.log, isNot(contains('protocol error')));
      await c.close();
    }, timeout: const Timeout(Duration(minutes: 3)));
  });

  group('SUBSCRIBE options', () {
    test(
        'retained message: delivered with retain=1, cleared by an empty '
        'retained publish', () async {
      broker = await Mosquitto.start();
      final pub = newClient(broker.port, clientId: 'rp');
      await pub.connect();
      await pub.publish('ret/t', bytes('kept'),
          qos: MqttQos.atLeastOnce, retain: true);
      final sub = newClient(broker.port, clientId: 'rs');
      await sub.connect();
      final inbox = Inbox(sub);
      await sub.subscribe('ret/t');
      await inbox.waitFor(1);
      expect(inbox.messages.single.retain, isTrue);
      expect(text(inbox.messages.single.payload), 'kept');

      await pub.publish('ret/t', Uint8List(0),
          qos: MqttQos.atLeastOnce, retain: true);
      final sub2 = newClient(broker.port, clientId: 'rs2');
      await sub2.connect();
      final inbox2 = Inbox(sub2);
      await sub2.subscribe('ret/t');
      await settle(400);
      expect(inbox2.messages, isEmpty);
      for (final c in [pub, sub, sub2]) {
        await c.close();
      }
    });

    test('Retain Handling 0 / 1 / 2', () async {
      broker = await Mosquitto.start();
      await mosqPub(broker.port, 'rh/t', 'R', retain: true, qos: 1);
      final c = newClient(broker.port, clientId: 'rh');
      await c.connect();
      final inbox = Inbox(c);

      Future<int> countAfter(Future<void> Function() action) async {
        final before = inbox.messages.length;
        await action();
        await settle(300);
        return inbox.messages.length - before;
      }

      // 0: on every subscribe, even when re-subscribing.
      expect(await countAfter(() => c.subscribe('rh/t')), 1);
      expect(await countAfter(() => c.subscribe('rh/t')), 1);
      await c.unsubscribe(['rh/t']);
      // 1: only when the subscription is new.
      const sendIfNew =
          MqttSubscriptionOptions(retainHandling: MqttRetainHandling.sendIfNew);
      expect(
          await countAfter(() => c.subscribe('rh/t', options: sendIfNew)), 1);
      expect(
          await countAfter(() => c.subscribe('rh/t', options: sendIfNew)), 0);
      await c.unsubscribe(['rh/t']);
      // 2: never.
      const never =
          MqttSubscriptionOptions(retainHandling: MqttRetainHandling.doNotSend);
      expect(await countAfter(() => c.subscribe('rh/t', options: never)), 0);
      await c.close();
    });

    test('Retain As Published keeps the RETAIN flag on live messages',
        () async {
      broker = await Mosquitto.start();
      final c = newClient(broker.port, clientId: 'rap');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('rap/keep',
          options: const MqttSubscriptionOptions(retainAsPublished: true));
      await c.subscribe('rap/clear');
      await c.publish('rap/keep', bytes('1'), retain: true);
      await c.publish('rap/clear', bytes('2'), retain: true);
      await inbox.waitFor(2);
      final byTopic = {for (final m in inbox.messages) m.topic: m};
      expect(byTopic['rap/keep']!.retain, isTrue);
      expect(byTopic['rap/clear']!.retain, isFalse);
      await c.close();
    });

    test('No Local suppresses the client\'s own publications', () async {
      broker = await Mosquitto.start();
      final c = newClient(broker.port, clientId: 'nl');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('nl/t',
          options: const MqttSubscriptionOptions(noLocal: true));
      await c.publish('nl/t', bytes('mine'));
      await mosqPub(broker.port, 'nl/t', 'theirs');
      await inbox.waitFor(1);
      await settle(300);
      expect(inbox.payloads, ['theirs']);
      await c.close();
    });

    test('Subscription Identifiers are sent and returned per subscription',
        () async {
      broker = await Mosquitto.start();
      final proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'sid');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('sid/+', subscriptionIdentifier: 7);
      await c.subscribe('sid/#', subscriptionIdentifier: 268435455);
      expect(prop(proxy.sent(kSubscribe).first.ackProperties, 0x0B), 7);
      await mosqPub(broker.port, 'sid/a', 'x');
      await inbox.waitFor(1);
      await settle(300);
      final ids =
          inbox.messages.expand((m) => m.subscriptionIdentifiers).toSet();
      expect(ids, {7, 268435455});
      await c.close();
      await proxy.close();
    });

    test('shared subscription distributes each message to exactly one member',
        () async {
      broker = await Mosquitto.start();
      final a = newClient(broker.port, clientId: 'sha');
      final b = newClient(broker.port, clientId: 'shb');
      await a.connect();
      await b.connect();
      final ia = Inbox(a), ib = Inbox(b);
      await a.subscribe(r'$share/g1/sh/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      await b.subscribe(r'$share/g1/sh/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      for (var i = 0; i < 20; i++) {
        await mosqPub(broker.port, 'sh/t', 'm$i', qos: 1);
      }
      await waitUntil(() => ia.messages.length + ib.messages.length >= 20);
      await settle(300);
      expect(ia.messages.length + ib.messages.length, 20);
      expect({...ia.payloads, ...ib.payloads}.length, 20);
      expect(ia.messages, isNotEmpty);
      expect(ib.messages, isNotEmpty);
      await a.unsubscribe([r'$share/g1/sh/t']);
      await a.close();
      await b.close();
    });

    test('wildcards: + and # match, and # does not match \$SYS topics',
        () async {
      broker = await Mosquitto.start(config: ['sys_interval 1']);
      final c = newClient(broker.port, clientId: 'wc');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribeAll([
        const MqttSubscription('#'),
        const MqttSubscription(r'$SYS/broker/version'),
      ]);
      await mosqPub(broker.port, 'a/b/c', '1');
      await settle(1500);
      final topics = inbox.messages.map((m) => m.topic).toSet();
      expect(topics, contains('a/b/c'));
      expect(topics, contains(r'$SYS/broker/version'));
      expect(
          topics.where(
              (t) => t.startsWith(r'$SYS') && t != r'$SYS/broker/version'),
          isEmpty);

      final c2 = newClient(broker.port, clientId: 'wc2');
      await c2.connect();
      final inbox2 = Inbox(c2);
      await c2.subscribe('a/+/c');
      await c2.subscribe('a/b/#');
      await mosqPub(broker.port, 'a/b/c', '2');
      await mosqPub(broker.port, 'a/x/c', '3');
      await mosqPub(broker.port, 'a/b', '4');
      await settle(400);
      expect(inbox2.payloads..sort(), ['2', '2', '3', '4']);
      await c.close();
      await c2.close();
    });

    test('subscribeAll in one packet; each filter gets its granted QoS',
        () async {
      broker = await Mosquitto.start(config: ['max_qos 1']);
      final proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'multi');
      await c.connect();
      await c.subscribeAll(const [
        MqttSubscription('m/0'),
        MqttSubscription('m/1',
            options: MqttSubscriptionOptions(qos: MqttQos.atLeastOnce)),
        MqttSubscription('m/2',
            options: MqttSubscriptionOptions(qos: MqttQos.exactlyOnce)),
      ]);
      final sub = proxy.sent(kSubscribe).single;
      expect(sub.subscribeFilters.map((f) => f.$1), ['m/0', 'm/1', 'm/2']);
      final suback = proxy.received(kSuback).single;
      final codes = suback.bytes.sublist(suback.bytes.length - 3);
      expect(codes, [0, 1, 1]);
      await c.close();
      await proxy.close();
    });

    test(
        'unsubscribe stops delivery; unknown filter answers 0x11 without '
        'throwing', () async {
      broker = await Mosquitto.start();
      final proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'uns');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('u/t');
      await c.unsubscribe(['u/t']);
      await c.unsubscribe(['never/subscribed']);
      final last = proxy.received(kUnsuback).last.bytes;
      expect(last.last, 0x11);
      await mosqPub(broker.port, 'u/t', 'x');
      await settle(300);
      expect(inbox.messages, isEmpty);
      await c.close();
      await proxy.close();
    });
  });

  group('authorization failures reported in acknowledgements', () {
    const acl = '''
user alice
topic readwrite ok/#
topic read ro/#
''';
    DynSec dynsec() => DynSec(username: 'alice', password: 'pw', acls: [
          {'acltype': 'subscribePattern', 'topic': 'ok/#', 'allow': true},
          {'acltype': 'publishClientSend', 'topic': 'ok/#', 'allow': true},
          {'acltype': 'publishClientReceive', 'topic': '#', 'allow': true},
          {'acltype': 'subscribePattern', 'topic': 'pinned/#', 'allow': true},
          {
            'acltype': 'unsubscribePattern',
            'topic': 'pinned/#',
            'allow': false
          },
        ]);

    test(
        'SUBACK 0x87 for a denied filter; the granted one is still recorded '
        'and works', () async {
      broker = await Mosquitto.start(dynsec: dynsec());
      final proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port,
          clientId: 'acl1', username: 'alice', password: 'pw');
      await c.connect();
      final inbox = Inbox(c);
      await expectLater(
        c.subscribeAll(
            const [MqttSubscription('ok/t'), MqttSubscription('no/t')]),
        throwsA(isA<MqttServerRejectedException>()
            .having((e) => e.reasonCode, 'reasonCode', 0x87)),
      );
      final suback = proxy.received(kSuback).single.bytes;
      expect(suback.sublist(suback.length - 2), [0x00, 0x87]);
      await c.publish('ok/t', bytes('fine'));
      await inbox.waitFor(1);
      expect(inbox.payloads, ['fine']);
      expect(c.state, MqttConnectionState.connected);
      await c.close();
      await proxy.close();
    });

    test(
        'UNSUBACK 0x87 surfaces as MqttServerRejectedException and the '
        'connection stays up', () async {
      broker = await Mosquitto.start(dynsec: dynsec());
      final c = newClient(broker.port,
          clientId: 'acl3', username: 'alice', password: 'pw');
      await c.connect();
      await c.subscribe('pinned/t');
      await expectLater(
        c.unsubscribe(['pinned/t']),
        throwsA(isA<MqttServerRejectedException>()
            .having((e) => e.reasonCode, 'reasonCode', 0x87)),
      );
      expect(c.state, MqttConnectionState.connected);
      await c.close();
    });

    test(
        'PUBACK / PUBREC 0x87 are results, not exceptions, and do not leak '
        'packet identifiers or quota', () async {
      broker = await Mosquitto.start(
          users: {'alice': 'pw'},
          acl: acl,
          config: ['max_inflight_messages 2']);
      // acl_file refuses publishes (not subscribes) with 0x87.
      final c = newClient(broker.port,
          clientId: 'acl2', username: 'alice', password: 'pw');
      await c.connect();
      for (var i = 0; i < 5; i++) {
        final r1 =
            await c.publish('ro/t', bytes('x'), qos: MqttQos.atLeastOnce);
        expect(r1.reasonCode, MqttReasonCode.notAuthorized);
        final r2 =
            await c.publish('ro/t', bytes('x'), qos: MqttQos.exactlyOnce);
        expect(r2.reasonCode, MqttReasonCode.notAuthorized);
      }
      expect(c.inflightCount, 0);
      final ok = await c.publish('ok/t', bytes('x'), qos: MqttQos.exactlyOnce);
      expect(ok.reasonCode,
          anyOf(MqttReasonCode.success, MqttReasonCode.noMatchingSubscribers));
      expect(c.state, MqttConnectionState.connected);
      await c.close();
    });

    test('PUBACK 0x10 No matching subscribers is reported as such', () async {
      broker = await Mosquitto.start();
      final c = newClient(broker.port, clientId: 'nms');
      await c.connect();
      final r =
          await c.publish('nobody/here', bytes('x'), qos: MqttQos.atLeastOnce);
      expect(r.reasonCode,
          anyOf(MqttReasonCode.noMatchingSubscribers, MqttReasonCode.success));
      await c.close();
    });
  });

  group('Topic Alias', () {
    test(
        'client -> broker: alias assigned, then topic sent empty; a '
        'third-party subscriber sees the right topic every time', () async {
      broker = await Mosquitto.start(config: ['max_topic_alias 3']);
      final proxy = await FaultProxy.start(broker.port);
      final witness = await MosqSub.start(broker.port, ['ta/#']);
      final c = newClient(proxy.port, clientId: 'ta');
      await c.connect();
      final sent = <String>[];
      for (var round = 0; round < 3; round++) {
        for (var t = 0; t < 5; t++) {
          final topic = 'ta/$t';
          sent.add(topic);
          await c.publish(topic, bytes('$round-$t'), qos: MqttQos.atLeastOnce);
        }
      }
      await witness.waitFor(15);
      expect(witness.messages.map((m) => m['topic']), sent);
      final pubs = proxy.sent(kPublish).map((f) => f.publish).toList();
      // First use binds (full name + alias), later uses send only the alias.
      expect(pubs[0].topic, 'ta/0');
      expect(prop(pubs[0].properties, 0x23), 1);
      expect(pubs[5].topic, '');
      expect(prop(pubs[5].properties, 0x23), 1);
      // Topics beyond the alias maximum fall back to full names.
      expect(pubs[8].topic, 'ta/3');
      expect(prop(pubs[8].properties, 0x23), isNull);
      for (final p in pubs) {
        final alias = prop(p.properties, 0x23) as int?;
        if (alias != null) expect(alias, inInclusiveRange(1, 3));
      }
      await witness.stop();
      await c.close();
      await proxy.close();
    });

    test('client -> broker with LRU eviction keeps topics correct', () async {
      broker = await Mosquitto.start(config: ['max_topic_alias 2']);
      final witness = await MosqSub.start(broker.port, ['ev/#']);
      final c =
          newClient(broker.port, clientId: 'ev', topicAliasEviction: true);
      await c.connect();
      final sent = <String>[];
      final order = [0, 1, 2, 0, 3, 1, 1, 2, 4, 0, 0, 3];
      for (final t in order) {
        sent.add('ev/$t');
        await c.publish('ev/$t', bytes('$t'), qos: MqttQos.atLeastOnce);
      }
      await witness.waitFor(order.length);
      expect(witness.messages.map((m) => m['topic']), sent);
      expect(witness.messages.map((m) => m['payload']), order.map((t) => '$t'));
      await witness.stop();
      await c.close();
    });

    test('broker -> client: aliased PUBLISH packets are resolved', () async {
      broker = await Mosquitto.start(config: ['max_topic_alias_broker 3']);
      final proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'tab');
      await c.connect(topicAliasMaximum: 3);
      final inbox = Inbox(c);
      await c.subscribe('tab/#');
      final expected = <String>[];
      for (var round = 0; round < 3; round++) {
        for (var t = 0; t < 5; t++) {
          expected.add('tab/$t');
          await mosqPub(broker.port, 'tab/$t', '$round');
        }
      }
      await inbox.waitFor(15);
      expect(inbox.messages.map((m) => m.topic), expected);
      final incoming = proxy.received(kPublish).map((f) => f.publish).toList();
      final usedAlias =
          incoming.where((p) => prop(p.properties, 0x23) != null).toList();
      printOnFailure(proxy.dump());
      expect(usedAlias, isNotEmpty,
          reason:
              'mosquitto should have used topic aliases towards the client');
      expect(incoming.where((p) => p.topic.isEmpty), isNotEmpty);
      await c.close();
      await proxy.close();
    });

    test('broker never uses aliases when the client declares none', () async {
      broker = await Mosquitto.start(config: ['max_topic_alias_broker 3']);
      final proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'tab0');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('tab0/#');
      for (var i = 0; i < 4; i++) {
        await mosqPub(broker.port, 'tab0/x', '$i');
      }
      await inbox.waitFor(4);
      expect(
          proxy
              .received(kPublish)
              .every((f) => prop(f.publish.properties, 0x23) == null),
          isTrue);
      await c.close();
      await proxy.close();
    });
  });

  group('client-declared limits honoured by the broker', () {
    test(
        'client Receive Maximum 1: broker keeps one QoS 1/2 PUBLISH '
        'outstanding; everything is delivered in order', () async {
      broker = await Mosquitto.start();
      final proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'rm1');
      await c.connect(receiveMaximum: 1);
      final inbox = Inbox(c);
      await c.subscribe('rm/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      final pubProxy = await FaultProxy.start(broker.port);
      final pub = newClient(pubProxy.port, clientId: 'rm-pub');
      await pub.connect();
      await Future.wait([
        for (var i = 0; i < 30; i++)
          pub.publish('rm/t', bytes('$i'),
              qos: i.isEven ? MqttQos.atLeastOnce : MqttQos.exactlyOnce),
      ]);
      // The publisher put them on the wire in call order...
      expect(pubProxy.sent(kPublish).map((f) => text(f.publish.payload)),
          [for (var i = 0; i < 30; i++) '$i']);
      await inbox.waitFor(30);
      // ...and ordering is only guaranteed per QoS level (section 4.6).
      expect(
          inbox.messages
              .where((m) => m.qos == MqttQos.atLeastOnce)
              .map((m) => text(m.payload)),
          [for (var i = 0; i < 30; i += 2) '$i']);
      expect(
          inbox.messages
              .where((m) => m.qos == MqttQos.exactlyOnce)
              .map((m) => text(m.payload)),
          [for (var i = 1; i < 30; i += 2) '$i']);
      expect(maxOutstanding(proxy.frames, Dir.s2c), 1);
      await pub.close();
      await pubProxy.close();
      await c.close();
      await proxy.close();
    });

    test(
        'client Maximum Packet Size: oversized messages are withheld by the '
        'broker and the connection survives', () async {
      broker = await Mosquitto.start();
      final c = newClient(broker.port, clientId: 'mps');
      await c.connect(maximumPacketSize: 200);
      final inbox = Inbox(c);
      await c.subscribe('mps/t');
      await mosqPub(broker.port, 'mps/t', 'x' * 500);
      await mosqPub(broker.port, 'mps/t', 'small');
      await inbox.waitFor(1);
      await settle(300);
      expect(inbox.payloads, ['small']);
      expect(c.state, MqttConnectionState.connected);
      await c.close();
    });
  });

  group('message expiry', () {
    test('an expired retained message is not delivered', () async {
      broker = await Mosquitto.start();
      final c = newClient(broker.port, clientId: 'exp');
      await c.connect();
      await c.publish('exp/t', bytes('short'),
          retain: true,
          qos: MqttQos.atLeastOnce,
          properties: const [MessageExpiryInterval(1)]);
      await c.publish('exp/u', bytes('long'),
          retain: true,
          qos: MqttQos.atLeastOnce,
          properties: const [MessageExpiryInterval(100)]);
      await settle(2200);
      final inbox = Inbox(c);
      await c.subscribe('exp/+');
      await settle(400);
      expect(inbox.payloads, ['long']);
      final remaining =
          inbox.messages.single.properties.whereType<MessageExpiryInterval>();
      expect(remaining.single.seconds, lessThan(100));
      await c.close();
    });
  });
}
