@Tags(['real-broker'])
@Timeout(Duration(seconds: 90))
library;

import 'dart:async';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

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

  Future<void> reconnected(MqttClient c, int connections) => waitUntil(
      () =>
          proxy.connections >= connections &&
          c.state == MqttConnectionState.connected,
      reason: 'reconnect #$connections');

  group('outgoing retransmission after a lost connection (section 4.4)', () {
    test(
        'QoS 1 PUBLISH whose PUBACK was lost is re-sent with DUP=1 and the '
        'same packet identifier', () async {
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      final witness = await MosqSub.start(broker.port, ['r1/t'], qos: 1);
      final c = newClient(proxy.port, clientId: 'r1');
      await c.connect(sessionExpiryInterval: _sei);
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
      final pending =
          c.publish('r1/t', bytes('once'), qos: MqttQos.atLeastOnce);
      await proxy.next((f) => f.dropped && f.type == kPuback);
      proxy.dropWhen = null;
      proxy.cutAll();
      final result = await pending.timeout(const Duration(seconds: 10));
      expect(result.reasonCode, MqttReasonCode.success);
      final pubs = proxy.sent(kPublish).toList();
      expect(pubs, hasLength(2));
      expect(pubs[0].dup, isFalse);
      expect(pubs[1].dup, isTrue);
      expect(pubs[1].packetId, pubs[0].packetId);
      expect(pubs[1].connection, 2);
      await broker.waitForLog('Received PUBLISH from r1 (d1, q1');
      expect(c.inflightCount, 0);
      await witness.waitFor(1);
      await witness.stop();
      await c.close();
    });

    test(
        'QoS 2 PUBLISH whose PUBREC was lost is re-sent with DUP=1 and '
        'delivered exactly once', () async {
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      final witness = await MosqSub.start(broker.port, ['r2/t'], qos: 2);
      final c = newClient(proxy.port, clientId: 'r2');
      await c.connect(sessionExpiryInterval: _sei);
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPubrec;
      final pending =
          c.publish('r2/t', bytes('exactly'), qos: MqttQos.exactlyOnce);
      await proxy.next((f) => f.dropped && f.type == kPubrec);
      proxy.dropWhen = null;
      proxy.cutAll();
      expect((await pending.timeout(const Duration(seconds: 10))).reasonCode,
          MqttReasonCode.success);
      final pubs = proxy.sent(kPublish).toList();
      expect(pubs.map((p) => p.dup), [false, true]);
      expect(pubs[1].packetId, pubs[0].packetId);
      await settle(500);
      expect(witness.messages, hasLength(1));
      await witness.stop();
      await c.close();
    });

    test(
        'QoS 2 exchange whose PUBCOMP was lost resumes with PUBREL, not a '
        'new PUBLISH', () async {
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      final witness = await MosqSub.start(broker.port, ['r3/t'], qos: 2);
      final c = newClient(proxy.port, clientId: 'r3');
      await c.connect(sessionExpiryInterval: _sei);
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPubcomp;
      final pending = c.publish('r3/t', bytes('x'), qos: MqttQos.exactlyOnce);
      await proxy.next((f) => f.dropped && f.type == kPubcomp);
      proxy.dropWhen = null;
      proxy.cutAll();
      expect((await pending.timeout(const Duration(seconds: 10))).reasonCode,
          MqttReasonCode.success);
      expect(proxy.sent(kPublish), hasLength(1));
      final pubrels = proxy.sent(kPubrel).toList();
      expect(pubrels.map((f) => f.connection), [1, 2]);
      await settle(500);
      expect(witness.messages, hasLength(1));
      await witness.stop();
      await c.close();
    });

    test(
        'a resumed backlog larger than the new connection\'s Receive '
        'Maximum is drained within the new quota, in publish order', () async {
      broker = await Mosquitto.start(
          persistence: true, config: ['max_inflight_messages 10']);
      proxy = await FaultProxy.start(broker.port);
      final witness = await MosqSub.start(broker.port, ['r4/t'], qos: 1);
      final c = newClient(proxy.port, clientId: 'r4');
      await c.connect(sessionExpiryInterval: _sei);
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
      final futures = [
        for (var i = 0; i < 10; i++)
          c.publish('r4/t', bytes('$i'), qos: MqttQos.atLeastOnce),
      ];
      await waitUntil(() => proxy.sent(kPublish).length == 10);
      expect(c.inflightCount, 10);
      await witness.stop();
      // Same session store, smaller Receive Maximum on the next connection.
      await broker.stop();
      proxy.dropWhen = null;
      broker = await Mosquitto.start(
          persistence: true,
          port: broker.port,
          dir: broker.dir,
          config: ['max_inflight_messages 3']);
      final results =
          await Future.wait(futures).timeout(const Duration(seconds: 15));
      expect(
          results.every((r) =>
              r.reasonCode == MqttReasonCode.success ||
              r.reasonCode == MqttReasonCode.noMatchingSubscribers),
          isTrue);
      expect(c.sessionPresent, isTrue);
      expect(c.serverCapabilities.receiveMaximum, 3);
      final resumed = proxy.frames.where((f) => f.connection >= 2).toList();
      final resent =
          resumed.where((f) => f.dir == Dir.c2s && f.type == kPublish).toList();
      expect(resent.map((f) => text(f.publish.payload)),
          [for (var i = 0; i < 10; i++) '$i']);
      expect(resent.every((f) => f.dup), isTrue);
      expect(maxOutstandingIn(resumed), lessThanOrEqualTo(3));
      expect(c.inflightCount, 0);
      await c.close();
    });

    test(
        'an unacknowledged publish stays in the session past operationTimeout '
        'and is retransmitted on the next connection', () async {
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port,
          clientId: 'r5', operationTimeout: const Duration(milliseconds: 400));
      await c.connect(sessionExpiryInterval: _sei);
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
      final pending = c.publish('r5/t', bytes('x'), qos: MqttQos.atLeastOnce);
      await Future<void>.delayed(const Duration(milliseconds: 500));
      expect(c.inflightCount, 1);
      proxy.dropWhen = null;
      proxy.cutAll();
      await reconnected(c, 2);
      await pending;
      expect(proxy.sent(kPublish).last.dup, isTrue);
      await c.close();
    });

    test(
        'without a session (expiry 0) an unacknowledged publish fails with '
        'a connection error after reconnect', () async {
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'r6');
      await c.connect();
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
      final pending = c.publish('r6/t', bytes('x'), qos: MqttQos.atLeastOnce);
      await proxy.next((f) => f.dropped && f.type == kPuback);
      proxy.dropWhen = null;
      proxy.cutAll();
      await expectLater(pending, throwsA(isA<MqttConnectionException>()));
      await reconnected(c, 2);
      expect(c.inflightCount, 0);
      await c.close();
    });
  });

  group('incoming QoS 2 duplicate suppression across reconnect', () {
    test(
        'a PUBLISH re-sent by the broker (our PUBREC was lost) is not '
        'delivered twice', () async {
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'in1');
      await c.connect(sessionExpiryInterval: _sei);
      final inbox = Inbox(c);
      await c.subscribe('in1/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      proxy.dropWhen = (f) => f.dir == Dir.c2s && f.type == kPubrec;
      await mosqPub(broker.port, 'in1/t', 'once', qos: 2);
      await proxy.next((f) => f.dropped && f.type == kPubrec);
      proxy.dropWhen = null;
      proxy.cutAll();
      await reconnected(c, 2);
      await broker.waitForLog('Received PUBCOMP from in1');
      expect(proxy.received(kPublish).map((f) => f.dup), [false, true]);
      await settle(300);
      expect(inbox.payloads, ['once']);
      await c.close();
    });

    test('a PUBREL re-sent by the broker (our PUBCOMP was lost) is completed',
        () async {
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'in2');
      await c.connect(sessionExpiryInterval: _sei);
      final inbox = Inbox(c);
      await c.subscribe('in2/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      proxy.dropWhen = (f) => f.dir == Dir.c2s && f.type == kPubcomp;
      await mosqPub(broker.port, 'in2/t', 'once', qos: 2);
      await proxy.next((f) => f.dropped && f.type == kPubcomp);
      proxy.dropWhen = null;
      proxy.cutAll();
      await reconnected(c, 2);
      await broker.waitForLog(RegExp(r'Received PUBCOMP from in2'));
      final resentPubrel =
          proxy.received(kPubrel).where((f) => f.connection == 2).single;
      final answer =
          proxy.sent(kPubcomp).where((f) => f.connection == 2).single;
      expect(answer.packetId, resentPubrel.packetId);
      printOnFailure('PUBCOMP reason code on retry: ${answer.ackReasonCode}');
      expect(inbox.payloads, ['once']);
      await c.close();
    });
  });

  group('session survives broker restarts', () {
    test(
        'persistent session: broker restart, client resumes without '
        're-subscribing and receives what was queued', () async {
      broker = await Mosquitto.start(persistence: true);
      proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'p1');
      await c.connect(sessionExpiryInterval: _sei);
      final inbox = Inbox(c);
      await c.subscribe('p1/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      await broker.restart();
      await waitUntil(
          () =>
              c.state == MqttConnectionState.connected &&
              proxy.connections >= 2,
          timeout: const Duration(seconds: 15));
      expect(c.sessionPresent, isTrue);
      expect(proxy.sent(kSubscribe), hasLength(1));
      await mosqPub(broker.port, 'p1/t', 'after-restart', qos: 1);
      await inbox.waitFor(1);
      await c.close();
    });

    test('lost session (no persistence): client re-subscribes on its own',
        () async {
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'p2');
      await c.connect(sessionExpiryInterval: _sei);
      final inbox = Inbox(c);
      await c.subscribeAll(const [
        MqttSubscription('p2/a',
            options: MqttSubscriptionOptions(qos: MqttQos.atLeastOnce)),
        MqttSubscription('p2/b'),
      ]);
      await c.subscribe('p2/c', subscriptionIdentifier: 5);
      await broker.restart();
      await waitUntil(() => proxy.sent(kSubscribe).length >= 4,
          timeout: const Duration(seconds: 15));
      expect(c.sessionPresent, isFalse);
      await settle(300);
      await mosqPub(broker.port, 'p2/a', 'a');
      await mosqPub(broker.port, 'p2/b', 'b');
      await mosqPub(broker.port, 'p2/c', 'c');
      await inbox.waitFor(3);
      expect(inbox.payloads..sort(), ['a', 'b', 'c']);
      expect(
          inbox.messages
              .firstWhere((m) => m.topic == 'p2/c')
              .subscriptionIdentifiers,
          [5]);
      await c.close();
    });

    test('graceful broker shutdown (DISCONNECT 0x8B) triggers a reconnect',
        () async {
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'p3');
      await c.connect();
      await broker.restart();
      await reconnected(c, 2);
      printOnFailure(proxy.dump());
      final disc = proxy.received(kDisconnect).toList();
      if (disc.isNotEmpty) expect(disc.single.ackReasonCode, 0x8B);
      await c.close();
    });
  });

  group('dead link detection', () {
    test('a black-holed link is detected by keep alive and recovered',
        () async {
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port,
          clientId: 'k1', pingResponseTimeout: const Duration(seconds: 1));
      await c.connect(
          keepAlive: const Duration(seconds: 1),
          connackTimeout: const Duration(seconds: 1));
      final states = <MqttConnectionState>[];
      c.stateStream.listen(states.add);
      proxy.blackhole = true;
      final sw = Stopwatch()..start();
      await waitUntil(() => states.contains(MqttConnectionState.reconnecting),
          timeout: const Duration(seconds: 6));
      expect(sw.elapsedMilliseconds, lessThan(4000));
      proxy.blackhole = false;
      await waitUntil(() => c.state == MqttConnectionState.connected,
          timeout: const Duration(seconds: 10));
      await c.close();
    });

    for (final via in ['direct', 'proxy']) {
      test(
          'broker crash (SIGKILL) during a QoS 0 flood ($via): no uncaught '
          'errors, client reconnects when the broker returns', () async {
        broker = await Mosquitto.start();
        proxy = await FaultProxy.start(broker.port);
        final log = CollectingLogger();
        final c = newClient(via == 'direct' ? broker.port : proxy.port,
            clientId: 'k2', logger: log);
        addTearDown(() => printOnFailure('client log:\n$log\nproxy:\n'
            '${proxy.dump().split('\n').where((l) => !l.contains('PUBLISH')).join('\n')}'
            '\nbroker:\n${broker.log}'));
        await c.connect();
        var running = true;
        var sent = 0, failed = 0;
        final flood = () async {
          while (running) {
            try {
              await c.publish('k2/t', bytes('x' * 200));
              sent++;
            } on MqttException {
              failed++;
            }
            if ((sent + failed) % 20 == 0) {
              await Future<void>.delayed(Duration.zero);
            }
          }
        }();
        await settle(300);
        await broker.kill();
        await settle(800);
        broker = await Mosquitto.start(port: broker.port, dir: broker.dir);
        await waitUntil(
            () =>
                c.state == MqttConnectionState.connected &&
                broker.countLog('New client connected') >= 1,
            timeout: const Duration(seconds: 10));
        running = false;
        await flood;
        expect(sent, greaterThan(0));
        expect(failed, greaterThan(0));
        await c.close();
      });
    }

    test(
        'peer RST while writing (write to a dead socket) is not an uncaught '
        'error', () async {
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      final c = newClient(proxy.port, clientId: 'k3', autoReconnect: false);
      await c.connect();
      final errors = <Object>[];
      c.errors.listen(errors.add);
      proxy.cutAll();
      for (var i = 0; i < 200; i++) {
        try {
          await c.publish('k3/t', bytes('x' * 1000));
        } on MqttException {
          break;
        }
      }
      await waitUntil(() => c.state == MqttConnectionState.disconnected);
      await settle(300);
      expect(errors, isNotEmpty);
      await c.close();
    });
  });
}

int maxOutstandingIn(List<WireFrame> frames) {
  final open = <int>{};
  var max = 0;
  for (final f in frames) {
    if (f.dropped) continue;
    if (f.dir == Dir.c2s && f.type == kPublish && f.qos > 0) {
      open.add(f.packetId);
      if (open.length > max) max = open.length;
    } else if (f.dir == Dir.s2c && (f.type == kPuback || f.type == kPubcomp)) {
      open.remove(f.packetId);
    }
  }
  return max;
}
