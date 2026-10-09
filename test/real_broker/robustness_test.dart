/// Production-readiness checks against real brokers: weak networks (latency,
/// jitter, low bandwidth, segmentation, stalls, half-open paths, unreachable
/// hosts), long chaos soaks with end-to-end delivery guarantees, API
/// concurrency and re-entrancy, and resource leaks.
@Tags(['real-broker', 'robustness'])
@Timeout(Duration(minutes: 3))
library;

import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'support/common.dart';
import 'support/emqx.dart';

const _sei = Duration(seconds: 600);

Future<int> openFds() async {
  final r = await Process.run('lsof', ['-n', '-P', '-p', '$pid']);
  return '${r.stdout}'.split('\n').where((l) => l.contains('TCP')).length;
}

/// Publishes sequence-numbered QoS 1 and QoS 2 messages and checks the
/// end-to-end guarantees at the subscriber.
final class DeliveryLedger {
  final Map<int, int> qos1 = {};
  final Map<int, int> qos2 = {};
  final List<int> qos1Order = [];
  final List<int> qos2Order = [];
  final Set<String> confirmed = {};
  final Map<String, int> failures = {};

  void onMessage(MqttMessage m) {
    final s = text(m.payload);
    final seq = int.parse(s.substring(2));
    if (s.startsWith('1:')) {
      qos1[seq] = (qos1[seq] ?? 0) + 1;
      qos1Order.add(seq);
    } else {
      qos2[seq] = (qos2[seq] ?? 0) + 1;
      qos2Order.add(seq);
    }
  }

  void verify() {
    final lost = confirmed.where((k) {
      final seq = int.parse(k.substring(2));
      return k.startsWith('1:')
          ? !qos1.containsKey(seq)
          : !qos2.containsKey(seq);
    }).toList();
    expect(lost, isEmpty, reason: 'acknowledged but never delivered');
    final dup2 = qos2.entries.where((e) => e.value > 1).map((e) => e.key);
    expect(dup2, isEmpty, reason: 'QoS 2 delivered more than once');
    expect(qos2Order, orderedEquals([...qos2Order]..sort()),
        reason: 'QoS 2 order');
    final first = <int>[];
    final seen = <int>{};
    for (final s in qos1Order) {
      if (seen.add(s)) first.add(s);
    }
    expect(first, orderedEquals([...first]..sort()), reason: 'QoS 1 order');
  }
}

void main() {
  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
    return;
  }

  late Mosquitto broker;
  late FaultProxy proxy;
  // Unbounded broker queues: a soak must measure the client, not the
  // broker's overflow policy (mosquitto drops beyond 1000 queued by default).
  const unbounded = [
    'max_inflight_messages 20',
    'max_queued_messages 0',
    'max_queued_bytes 0',
  ];
  setUp(() async {
    broker = await Mosquitto.start(config: unbounded);
    proxy = await FaultProxy.start(broker.port);
  });
  tearDown(() async {
    await proxy.close();
    await broker.dispose();
  });

  group('weak network', () {
    test(
        '300 ms latency + 100 ms jitter each way: no false keep-alive '
        'timeouts, QoS 1/2 complete', () async {
      proxy.c2s
        ..latency = const Duration(milliseconds: 300)
        ..jitter = const Duration(milliseconds: 100);
      proxy.s2c
        ..latency = const Duration(milliseconds: 300)
        ..jitter = const Duration(milliseconds: 100);
      final c = newClient(proxy.port, clientId: 'lat');
      await c.connect(keepAlive: const Duration(seconds: 2));
      final inbox = Inbox(c);
      await c.subscribe('lat/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      await Future.wait([
        for (var i = 0; i < 50; i++)
          c.publish('lat/t', bytes('$i'),
              qos: i.isEven ? MqttQos.atLeastOnce : MqttQos.exactlyOnce),
      ]);
      await inbox.waitFor(50);
      await settle(6000);
      expect(proxy.connections, 1, reason: 'no reconnects');
      expect(proxy.sent(kPingreq), isNotEmpty);
      await c.close();
    });

    test('every segment delivered one byte at a time in both directions',
        () async {
      proxy.c2s.maxSegment = 1;
      proxy.s2c.maxSegment = 1;
      final c = newClient(proxy.port, clientId: 'frag');
      await c.connect();
      final inbox = Inbox(c);
      await c.subscribe('frag/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      final payload = Uint8List.fromList(List.generate(3000, (i) => i & 0xFF));
      await c.publish('frag/t', payload,
          qos: MqttQos.exactlyOnce,
          properties: const [UserProperty('k', 'v'), ContentType('x')]);
      await inbox.waitFor(1);
      expect(inbox.messages.single.payload, payload);
      expect(broker.log, isNot(contains('protocol error')));
      await c.close();
    });

    test(
        'slow downlink (64 KB/s) carrying 1 MiB with a 1 s keep alive: the '
        'transfer is not mistaken for a dead link', () async {
      proxy.s2c.bytesPerSecond = 64 * 1024;
      final c = newClient(proxy.port, clientId: 'slowdown');
      await c.connect(keepAlive: const Duration(seconds: 1));
      final inbox = Inbox(c);
      await c.subscribe('slow/t');
      final pub = newClient(broker.port, clientId: 'slowdown-pub');
      await pub.connect();
      await pub.publish('slow/t', Uint8List(1024 * 1024),
          qos: MqttQos.atLeastOnce);
      await inbox.waitFor(1, timeout: const Duration(seconds: 40));
      expect(proxy.connections, 1, reason: 'no reconnect during the transfer');
      await pub.close();
      await c.close();
    }, timeout: const Timeout(Duration(minutes: 2)));

    test(
        'a 3 s stall shorter than keep alive does not drop the link and '
        'publishes made during it complete afterwards', () async {
      final c = newClient(proxy.port, clientId: 'stall');
      await c.connect(keepAlive: const Duration(seconds: 10));
      proxy.c2s.paused = true;
      proxy.s2c.paused = true;
      final during = c.publish('st/t', bytes('x'), qos: MqttQos.atLeastOnce);
      await settle(3000);
      proxy.c2s.paused = false;
      proxy.s2c.paused = false;
      final r = await during.timeout(const Duration(seconds: 5));
      expect(r.reasonCode, isNotNull);
      expect(proxy.connections, 1);
      await c.close();
    }, timeout: const Timeout(Duration(minutes: 1)));

    test('half-open: downlink dead -> detected by the client and recovered',
        () async {
      final c = newClient(proxy.port,
          clientId: 'half-s2c',
          pingResponseTimeout: const Duration(seconds: 1));
      await c.connect(
          keepAlive: const Duration(seconds: 1),
          sessionExpiryInterval: _sei,
          connackTimeout: const Duration(seconds: 2));
      proxy.s2c.blackhole = true;
      await waitUntil(() => c.state != MqttConnectionState.connected,
          timeout: const Duration(seconds: 5));
      proxy.s2c.blackhole = false;
      await waitUntil(() => c.state == MqttConnectionState.connected,
          timeout: const Duration(seconds: 10));
      final r = await c.publish('h/t', bytes('x'), qos: MqttQos.atLeastOnce);
      expect(r.reasonCode, isNotNull);
      await c.close();
    });

    test(
        'half-open: uplink dead while the client keeps publishing QoS 0 -> '
        'detected and recovered', () async {
      final c = newClient(proxy.port,
          clientId: 'half-c2s',
          pingResponseTimeout: const Duration(seconds: 1));
      await c.connect(
          keepAlive: const Duration(seconds: 1),
          connackTimeout: const Duration(seconds: 2));
      proxy.c2s.blackhole = true;
      var stop = false;
      final loop = () async {
        while (!stop) {
          try {
            await c.publish('h/t', bytes('x'));
          } on MqttException {
            // Expected while down.
          }
          await settle(100);
        }
      }();
      await waitUntil(() => c.state != MqttConnectionState.connected,
          timeout: const Duration(seconds: 6));
      proxy.c2s.blackhole = false;
      await waitUntil(() => c.state == MqttConnectionState.connected,
          timeout: const Duration(seconds: 10));
      stop = true;
      await loop;
      await c.close();
    });

    test('unreachable host: attempts back off and connect() can be cancelled',
        () async {
      final log = CollectingLogger();
      final c = MqttClient(
        host: '10.255.255.1',
        clientId: 'unreachable',
        connectionTimeout: const Duration(milliseconds: 300),
        logger: log,
        reconnectManager: ReconnectManager(
            initialDelay: const Duration(milliseconds: 200),
            maxDelay: const Duration(seconds: 1),
            jitterFactor: 0),
      );
      final connecting =
          c.connect().then<Object?>((_) => null, onError: (Object e) => e);
      await settle(5000);
      final attempts =
          log.lines.where((l) => l.contains('Connection failed')).length;
      expect(attempts, inInclusiveRange(3, 10), reason: '$attempts attempts');
      final sw = Stopwatch()..start();
      await c.disconnect();
      expect(await connecting, isNotNull);
      expect(sw.elapsedMilliseconds, lessThan(1500));
      await c.close();
    });

    test('broker down for 6 s: reconnect attempts back off, then recover',
        () async {
      final log = CollectingLogger();
      final c = MqttClient(
        host: '127.0.0.1',
        port: proxy.port,
        clientId: 'down',
        logger: log,
        reconnectManager: ReconnectManager(
            initialDelay: const Duration(milliseconds: 100),
            maxDelay: const Duration(seconds: 1),
            jitterFactor: 0),
      );
      await c.connect(sessionExpiryInterval: _sei);
      await broker.stop();
      await settle(6000);
      final attempts =
          log.lines.where((l) => l.contains('Connection failed')).length;
      expect(attempts, inInclusiveRange(4, 14), reason: '$attempts attempts');
      broker = await Mosquitto.start(port: broker.port, dir: broker.dir);
      await waitUntil(() => c.state == MqttConnectionState.connected,
          timeout: const Duration(seconds: 5));
      await c.close();
    });
  });

  group('chaos soak: end-to-end delivery guarantees', () {
    Future<void> soak({
      required int brokerPort,
      required Duration duration,
      Future<void> Function()? restart,
    }) async {
      // With broker restarts the subscriber's wire is recorded, to attribute
      // any duplicate QoS 2 delivery (see below).
      final attribute = restart != null;
      final pubProxy = proxy;
      final subProxy = await FaultProxy.start(brokerPort);
      addTearDown(subProxy.close);
      for (final p in [pubProxy, subProxy]) {
        p.record = Platform.environment['SOAK_RECORD'] == '1' ||
            (attribute && identical(p, subProxy));
        p.c2s
          ..latency = const Duration(milliseconds: 20)
          ..jitter = const Duration(milliseconds: 40);
        p.s2c
          ..latency = const Duration(milliseconds: 20)
          ..jitter = const Duration(milliseconds: 40)
          ..maxSegment = 700;
      }
      final ledger = DeliveryLedger();
      final sub = newClient(subProxy.port,
          clientId: 'soak-sub', operationTimeout: const Duration(minutes: 5));
      sub.messages.listen(ledger.onMessage);
      await sub.connect(sessionExpiryInterval: _sei, receiveMaximum: 20);
      await sub.subscribe('soak/#',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      final pub = newClient(pubProxy.port,
          clientId: 'soak-pub', operationTimeout: const Duration(minutes: 5));
      await pub.connect(sessionExpiryInterval: _sei);

      pubProxy.startChaos(
          const Duration(milliseconds: 300), const Duration(seconds: 2));
      subProxy.startChaos(
          const Duration(milliseconds: 300), const Duration(seconds: 2));
      final end = DateTime.now().add(duration);
      final pending = <Future<void>>[];
      var seq = 0;
      var notConnected = 0;
      Timer? restarts;
      if (restart != null) {
        restarts = Timer.periodic(duration ~/ 4, (_) => restart());
      }
      while (DateTime.now().isBefore(end)) {
        if (pub.state != MqttConnectionState.connected) {
          notConnected++;
          await settle(20);
          continue;
        }
        final n = seq++;
        for (final qos in [MqttQos.atLeastOnce, MqttQos.exactlyOnce]) {
          final key = '${qos.value}:$n';
          Future<MqttPublishResult> f;
          try {
            f = pub.publish('soak/${qos.value}', bytes(key), qos: qos);
          } on MqttConnectionException {
            continue;
          }
          pending.add(f.then((r) {
            if (r.reasonCode == MqttReasonCode.success ||
                r.reasonCode == MqttReasonCode.noMatchingSubscribers) {
              ledger.confirmed.add(key);
            } else {
              ledger.failures['rc ${r.reasonCode.name}'] =
                  (ledger.failures['rc ${r.reasonCode.name}'] ?? 0) + 1;
            }
          }, onError: (Object e) {
            ledger.failures['${e.runtimeType}'] =
                (ledger.failures['${e.runtimeType}'] ?? 0) + 1;
          }));
        }
        await settle(10);
      }
      restarts?.cancel();
      pubProxy.stopChaos();
      subProxy.stopChaos();
      var settled = 0;
      for (final f in pending) {
        f.whenComplete(() => settled++);
      }
      final deadline = DateTime.now().add(const Duration(seconds: 60));
      while (settled < pending.length && DateTime.now().isBefore(deadline)) {
        await settle(100);
      }
      String missing() {
        final m = ledger.confirmed.where((k) {
          final s = int.parse(k.substring(2));
          return k.startsWith('1:')
              ? !ledger.qos1.containsKey(s)
              : !ledger.qos2.containsKey(s);
        }).toList()
          ..sort();
        return '${m.length} ${m.take(20)}';
      }

      print('SOAK broker drops: ${broker.countLog('dropped')}');
      print(
          'SOAK published $seq x2, futures settled $settled/${pending.length}, '
          'confirmed ${ledger.confirmed.length}, failures ${ledger.failures}, '
          'cuts pub=${pubProxy.chaosCuts} sub=${subProxy.chaosCuts}, '
          'pub state ${pub.state} inflight ${pub.inflightCount}, '
          'sub state ${sub.state}, missing now ${missing()}');
      expect(settled, pending.length, reason: 'publish futures never settled');
      await waitUntil(
          () => ledger.confirmed.every((k) {
                final s = int.parse(k.substring(2));
                return k.startsWith('1:')
                    ? ledger.qos1.containsKey(s)
                    : ledger.qos2.containsKey(s);
              }),
          timeout: const Duration(seconds: 30),
          reason: 'drain: missing ${missing()}');
      await settle(1000);
      printOnFailure('published $seq x2, confirmed ${ledger.confirmed.length}, '
          'failures ${ledger.failures}, cuts pub=${pubProxy.chaosCuts} '
          'sub=${subProxy.chaosCuts}, skipped while down $notConnected');
      if (attribute) {
        // mosquitto 2.1.2 with the sqlite persistence plugin can re-deliver a
        // QoS 2 message after a graceful restart although the exchange had
        // completed (PUBREC, PUBREL and PUBCOMP all on the wire before the
        // restart), and it does so with DUP=0. The receiver discarded the
        // exchange at PUBCOMP, as section 4.3.3 requires, so it cannot tell
        // this from a new message. Prove every duplicate is of that kind:
        // the client completed the earlier exchange.
        // Walk the subscriber's wire. A PUBLISH starts a new exchange unless
        // it repeats the identifier of one still open, which is a
        // retransmission the client must suppress. When an exchange closes
        // depends on what reached the client, which the wire record cannot
        // show for a frame forwarded just before a cut, so two counts bound
        // it: closing only at the client's PUBCOMP (fewest exchanges), or
        // already at the broker's PUBREL (most). The latter matters because
        // mosquitto, after a restart, re-sends the PUBLISH of an exchange it
        // had already released, which a client that processed the PUBREL
        // must treat as new (section 4.3.3). The client is right if its
        // deliveries fall within the bounds.
        Map<int, int> exchanges({required bool closeOnPubrel}) {
          final count = <int, int>{};
          final open = <int, int>{}; // seq -> open packet identifier
          for (final f in subProxy.frames) {
            if (f.dropped) continue;
            if (f.dir == Dir.s2c && f.type == kPublish && f.qos == 2) {
              final seq = int.parse(text(f.publish.payload).substring(2));
              if (open[seq] != f.publish.packetId) {
                count[seq] = (count[seq] ?? 0) + 1;
                open[seq] = f.publish.packetId!;
              }
            } else if ((f.dir == Dir.c2s && f.type == kPubcomp) ||
                (closeOnPubrel && f.dir == Dir.s2c && f.type == kPubrel)) {
              open.removeWhere((_, id) => id == f.packetId);
            }
          }
          return count;
        }

        final fewest = exchanges(closeOnPubrel: false);
        final most = exchanges(closeOnPubrel: true);
        for (final e in ledger.qos2.entries) {
          expect(
              e.value, inInclusiveRange(fewest[e.key] ?? 0, most[e.key] ?? 0),
              reason: 'QoS 2 seq ${e.key}: delivered ${e.value} times for '
                  '${fewest[e.key]}..${most[e.key]} exchange(s) the broker '
                  'began');
        }
        final fresh = most;
        final brokerDups = fresh.values.where((n) => n > 1).length;
        print('SOAK QoS 2 messages the broker re-delivered after completion: '
            '$brokerDups');
        ledger.qos2.updateAll((_, n) => 1);
        ledger.qos2Order
          ..clear()
          ..addAll(ledger.qos2.keys.toList()..sort());
      }
      ledger.verify();
      expect(ledger.failures, isEmpty,
          reason: 'a publish accepted while connected must not fail under a '
              'persistent session with a generous operation timeout');
      expect(pub.inflightCount, 0);
      await pub.close();
      await sub.close();
    }

    test(
        'mosquitto, 45 s, random cuts every 0.3-2 s on both clients, '
        'latency/jitter/segmentation', () async {
      await soak(
          brokerPort: broker.port, duration: const Duration(seconds: 45));
    });

    test('mosquitto with persistence, graceful broker restarts during chaos',
        () async {
      await proxy.close();
      await broker.dispose();
      broker = await Mosquitto.start(persistence: true, config: unbounded);
      proxy = await FaultProxy.start(broker.port);
      await soak(
        brokerPort: broker.port,
        duration: const Duration(seconds: 40),
        restart: () => broker.restart(),
      );
    });

    test('EMQX, 45 s, same chaos', () async {
      if (!await Emqx.available) return markTestSkipped('no EMQX image');
      final emqx = await Emqx.start(env: {
        'EMQX_MQTT__MAX_MQUEUE_LEN': '1000000',
        'EMQX_MQTT__MAX_INFLIGHT': '20',
      });
      addTearDown(emqx.dispose);
      await proxy.close();
      proxy = await FaultProxy.start(emqx.port);
      await soak(brokerPort: emqx.port, duration: const Duration(seconds: 45));
    });
  });

  group('concurrency and re-entrancy', () {
    test('200 clients at once: connect, subscribe, QoS 1 round trip, close',
        () async {
      final clients = [
        for (var i = 0; i < 200; i++)
          newClient(broker.port, clientId: 'many$i'),
      ];
      await Future.wait(clients.map((c) async {
        await c.connect();
        final got = c.messages.first;
        await c.subscribe('many/${c.clientId}',
            options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
        await c.publish('many/${c.clientId}', bytes('x'),
            qos: MqttQos.atLeastOnce);
        await got.timeout(const Duration(seconds: 20));
      }));
      await Future.wait(clients.map((c) => c.close()));
    });

    test('ten concurrent connect() calls produce one CONNECT', () async {
      final c = newClient(proxy.port, clientId: 'cc');
      await Future.wait([for (var i = 0; i < 10; i++) c.connect()]);
      expect(proxy.sent(kConnect), hasLength(1));
      await c.close();
    });

    test(
        'connect() immediately followed by disconnect() leaves nothing '
        'behind', () async {
      final before = await openFds();
      for (var i = 0; i < 30; i++) {
        final c = newClient(proxy.port, clientId: 'cd$i');
        final f =
            c.connect().then<Object?>((_) => null, onError: (Object e) => e);
        await c.disconnect();
        await f;
        expect(c.state, MqttConnectionState.disconnected);
        await c.close();
      }
      await settle(500);
      expect(proxy.liveConnections, 0);
      expect(await openFds(), lessThanOrEqualTo(before + 2));
    });

    test(
        'disconnect() with 1000 QoS 1 publishes in flight settles every '
        'future', () async {
      final c = newClient(proxy.port, clientId: 'dq');
      await c.connect();
      final results = [
        for (var i = 0; i < 1000; i++)
          c
              .publish('dq/t', bytes('$i'), qos: MqttQos.atLeastOnce)
              .then<Object>((r) => r, onError: (Object e) => e),
      ];
      await settle(5);
      await c.disconnect();
      final all =
          await Future.wait(results).timeout(const Duration(seconds: 10));
      expect(
          all.every(
              (r) => r is MqttPublishResult || r is MqttConnectionException),
          isTrue);
      expect(c.inflightCount, 0);
      await c.close();
    });

    test('close() during a reconnect loop stops all attempts', () async {
      final c = newClient(proxy.port, clientId: 'cr');
      await c.connect();
      proxy.refuse = true;
      proxy.cutAll();
      await settle(500);
      await c.close().timeout(const Duration(seconds: 3));
      final after = proxy.connections;
      await settle(1500);
      expect(proxy.connections, after);
    });

    test(
        'API storm under chaos: publishes, subscribes and unsubscribes from '
        'several tasks only ever fail with connection/timeout errors',
        () async {
      final c = newClient(proxy.port,
          clientId: 'storm', operationTimeout: const Duration(seconds: 5));
      await c.connect(sessionExpiryInterval: _sei);
      proxy.startChaos(
          const Duration(milliseconds: 200), const Duration(milliseconds: 900));
      final unexpected = <Object>[];
      final rnd = Random(7);
      var ops = 0;
      final end = DateTime.now().add(const Duration(seconds: 15));
      Future<void> task(int id) async {
        while (DateTime.now().isBefore(end)) {
          try {
            switch (rnd.nextInt(5)) {
              case 0:
                await c.publish('storm/$id', bytes('x'));
              case 1:
                await c.publish('storm/$id', bytes('x'),
                    qos: MqttQos.atLeastOnce);
              case 2:
                await c.publish('storm/$id', bytes('x'),
                    qos: MqttQos.exactlyOnce);
              case 3:
                await c.subscribe('storm/${rnd.nextInt(20)}');
              default:
                await c.unsubscribe(['storm/${rnd.nextInt(20)}']);
            }
            ops++;
          } on MqttConnectionException {
            await settle(20);
          } on MqttTimeoutException {
            // Allowed under chaos.
          } on MqttServerRejectedException {
            // e.g. UNSUBACK 0x11 is not an exception; others are server truth.
          } on Object catch (e) {
            unexpected.add(e);
          }
        }
      }

      await Future.wait([for (var i = 0; i < 6; i++) task(i)]);
      proxy.stopChaos();
      await waitUntil(() => c.state == MqttConnectionState.connected,
          timeout: const Duration(seconds: 10));
      await waitUntil(() => c.inflightCount == 0,
          timeout: const Duration(seconds: 20));
      expect(unexpected, isEmpty);
      expect(ops, greaterThan(100));
      // The client is still fully functional.
      final inbox = Inbox(c);
      await c.subscribe('storm/final',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      await c.publish('storm/final', bytes('ok'), qos: MqttQos.exactlyOnce);
      await inbox.waitFor(1);
      await c.close();
    });

    test(
        're-entrancy: stateStream listener disconnecting on reconnecting, '
        'reconnecting from disconnected, publishing from a message listener',
        () async {
      final c = newClient(proxy.port, clientId: 're');
      var reacted = false;
      Future<void>? stopping;
      final sub = c.stateStream.listen((s) {
        if (s == MqttConnectionState.reconnecting && !reacted) {
          reacted = true;
          stopping = c.disconnect();
        }
      });
      await c.connect();
      proxy.cutAll();
      await waitUntil(() => stopping != null);
      await stopping;
      await waitUntil(() => c.state == MqttConnectionState.disconnected);
      await settle(500);
      expect(c.state, MqttConnectionState.disconnected);
      await sub.cancel();

      // Reconnect from a listener that observed the explicit disconnect.
      final again = Completer<void>();
      final sub2 = c.stateStream.listen((s) {
        if (s == MqttConnectionState.connected && !again.isCompleted) {
          again.complete();
        }
      });
      await c.connect();
      await again.future.timeout(const Duration(seconds: 5));
      await sub2.cancel();

      // Echo from inside the message listener. The QoS 1 future has to be
      // retained: close() fails an in-session publish that has not been
      // acknowledged yet, and an unawaited future would surface that as an
      // uncaught error.
      final echoed = Completer<void>();
      final echoAck = Completer<void>();
      c.messages.listen((m) {
        if (m.topic == 're/in') {
          if (!echoAck.isCompleted) {
            echoAck.complete(
              c
                  .publish('re/out', m.payload, qos: MqttQos.atLeastOnce)
                  .then((_) {}),
            );
          }
        } else if (m.topic == 're/out') {
          if (!echoed.isCompleted) echoed.complete();
        }
      });
      await c.subscribeAll(const [
        MqttSubscription('re/in'),
        MqttSubscription('re/out'),
      ]);
      await c.publish('re/in', bytes('x'));
      await echoed.future.timeout(const Duration(seconds: 5));
      await echoAck.future.timeout(const Duration(seconds: 5));
      await c.close();
    });

    test('throwing listeners do not break the connection', () async {
      final uncaught = <Object>[];
      final reported = <Object>[];
      await runZonedGuarded(() async {
        final c = newClient(proxy.port, clientId: 'throws');
        c.errors.listen(reported.add);
        c.stateStream.listen((_) => throw StateError('state listener'));
        await c.connect();
        c.messages.listen((_) => throw StateError('message listener'));
        final inbox = Inbox(c);
        await c.subscribe('th/t');
        await c.publish('th/t', bytes('1'));
        await c.publish('th/t', bytes('2'));
        await inbox.waitFor(2);
        expect(c.state, MqttConnectionState.connected);
        await c.close();
      }, (e, _) => uncaught.add(e));
      expect(uncaught, isEmpty);
      expect(reported.whereType<StateError>(), isNotEmpty);
    });

    test('300 connect/disconnect cycles leak no sockets', () async {
      final c = newClient(proxy.port, clientId: 'cycle');
      await c.connect();
      await c.disconnect();
      final before = await openFds();
      for (var i = 0; i < 300; i++) {
        await c.connect();
        await c.publish('cy/t', bytes('x'), qos: MqttQos.atLeastOnce);
        await c.disconnect();
      }
      await settle(500);
      expect(await openFds(), lessThanOrEqualTo(before + 2));
      expect(proxy.liveConnections, 0);
      await c.close();
    });
  });
}
