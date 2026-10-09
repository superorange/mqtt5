/// 0.5.0 behaviour on a real mosquitto: session ownership, the
/// acknowledgement timeout, retry rules for automatic reconnects and
/// re-subscription after a lost session. The fault proxy only drops, cuts or
/// rewrites what the real broker sent.
@Tags(['real-broker'])
@Timeout(Duration(seconds: 120))
library;

import 'dart:async';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'support/common.dart';

const _sei = Duration(seconds: 60);
const _q1 = MqttSubscriptionOptions(qos: MqttQos.atLeastOnce);

void main() {
  late Mosquitto broker;
  late FaultProxy proxy;

  setUp(() async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
  });
  tearDown(() async {
    await proxy.close();
    await broker.dispose();
  });

  MqttClient client(String id,
      {Duration ackTimeout = const Duration(seconds: 60), MqttWill? will}) {
    final c = MqttClient(
      host: '127.0.0.1',
      port: proxy.port,
      clientId: id,
      ackTimeout: ackTimeout,
      will: will,
      operationTimeout: const Duration(seconds: 10),
      reconnectManager: fastReconnect(),
    );
    // Also when an expectation fails: no reconnect loop may outlive the test.
    addTearDown(c.close);
    return c;
  }

  test(
      'a new client object refuses an unowned session; adopting it delivers '
      'the offline backlog', () async {
    final id = 'own-${DateTime.now().microsecondsSinceEpoch}';
    final first = client(id);
    await first.connect(sessionExpiryInterval: _sei);
    await first.subscribe('own/t', options: _q1);
    await first.disconnect();
    await first.close();
    for (var i = 0; i < 3; i++) {
      await mosqPub(broker.port, 'own/t', 'm$i', qos: 1);
    }

    final second = client(id);
    await expectLater(
      second.connect(cleanStart: false, sessionExpiryInterval: _sei),
      throwsA(isA<MqttSessionNotOwnedException>()),
    );
    // Connection 1 ended with first's normal DISCONNECT; the refusal is on 2.
    final refusal = await proxy.next(
        (f) => f.dir == Dir.c2s && f.type == kDisconnect && f.connection == 2,
        includeHistory: true);
    expect(refusal.ackReasonCode, 0x82);
    await second.close();

    // The refusal did not cost the broker's session or its backlog.
    final third = client(id);
    await third.connect(
      cleanStart: false,
      sessionExpiryInterval: _sei,
      adoptBrokerSession: true,
    );
    expect(third.sessionPresent, isTrue);
    final inbox = Inbox(third);
    await inbox.waitFor(3);
    expect(inbox.payloads, ['m0', 'm1', 'm2']);
    await third.close();
  });

  test('ackTimeout replaces a connection that stops acknowledging', () async {
    final witness = await MosqSub.start(broker.port, ['ack/t', 'ack/will']);
    var dropped = false;
    proxy.dropWhen = (f) {
      if (!dropped && f.dir == Dir.s2c && f.type == kPuback) {
        dropped = true;
        return true;
      }
      return false;
    };
    final c = client(
      'ack-${DateTime.now().microsecondsSinceEpoch}',
      ackTimeout: const Duration(seconds: 1),
      will: MqttWill(topic: 'ack/will', payload: bytes('gone')),
    );
    await c.connect(sessionExpiryInterval: _sei);
    var failed = false;
    final result = await c
        .publish('ack/t', bytes('once'), qos: MqttQos.atLeastOnce)
        .catchError((Object e) {
      failed = true;
      throw e;
    });
    expect(failed, isFalse);
    expect(result.reasonCode, MqttReasonCode.success);
    expect(proxy.connections, 2);

    // The first connection was closed with DISCONNECT 0x00 (omitted or 0x00
    // on the wire): no Will, session kept. The second re-sent with DUP.
    final goodbye = proxy.sent(kDisconnect).single;
    expect(goodbye.connection, 1);
    expect(goodbye.ackReasonCode ?? 0, 0);
    final publishes = proxy.sent(kPublish).toList();
    expect(publishes.map((f) => f.connection), [1, 2]);
    expect(publishes.last.dup, isTrue);
    expect(publishes.last.packetId, publishes.first.packetId);

    await settle(500);
    final topics = witness.messages.map((m) => m['topic']).toList();
    expect(topics, isNot(contains('ack/will')));
    expect(topics, contains('ack/t'));
    await c.close();
  });

  test('a protocol error in the CONNACK read of a reconnect is retried',
      () async {
    proxy.rewrite = (f) {
      if (f.dir == Dir.s2c && f.type == kConnack && f.connection == 2) {
        // One write: CONNACK followed by a PINGREQ, which a server must never
        // send.
        return [
          Uint8List.fromList([...f.bytes, 0xC0, 0x00])
        ];
      }
      return null;
    };
    final c = client('proto-${DateTime.now().microsecondsSinceEpoch}');
    final errors = <Object>[];
    c.errors.listen(errors.add);
    await c.connect(sessionExpiryInterval: _sei);
    proxy.cutAll();
    await waitUntil(
      () => proxy.connections >= 3 && c.state == MqttConnectionState.connected,
      reason: 'reconnect after the bad CONNACK read',
    );
    final refusal =
        proxy.sent(kDisconnect).where((f) => f.connection == 2).single;
    expect(refusal.ackReasonCode, 0x82);
    expect(errors, isEmpty);
    await c.close();
  });

  test('CONNACK 0x85 on a reconnect is retried', () async {
    proxy.rewrite = (f) {
      if (f.dir == Dir.s2c && f.type == kConnack && f.connection == 2) {
        return [
          Uint8List.fromList([0x20, 0x03, 0x00, 0x85, 0x00])
        ];
      }
      return null;
    };
    final c = client('id85-${DateTime.now().microsecondsSinceEpoch}');
    final errors = <Object>[];
    c.errors.listen(errors.add);
    await c.connect(sessionExpiryInterval: _sei);
    proxy.cutAll();
    await waitUntil(
      () => proxy.connections >= 3 && c.state == MqttConnectionState.connected,
      reason: 'reconnect after CONNACK 0x85',
    );
    expect(errors, isEmpty);
    await c.close();
  });

  test('a re-subscription cut short is completed on the next connection',
      () async {
    final c = client('resub-${DateTime.now().microsecondsSinceEpoch}');
    await c.connect(sessionExpiryInterval: _sei);
    await c.subscribe('resub/t', options: _q1);
    final inbox = Inbox(c);

    // mosquitto without persistence loses the session on restart. The
    // re-subscription on the next connection is dropped and that connection
    // cut, so the broker keeps a session with no subscription.
    var cutDone = false;
    proxy.dropWhen = (f) {
      if (!cutDone &&
          f.dir == Dir.c2s &&
          f.type == kSubscribe &&
          f.connection >= 2) {
        cutDone = true;
        Timer(const Duration(milliseconds: 50), proxy.cutAll);
        return true;
      }
      return false;
    };
    await broker.restart();
    await waitUntil(() => cutDone, reason: 're-subscription seen');
    await waitUntil(
      () => c.state == MqttConnectionState.connected && c.sessionPresent,
      reason: 'resumed the session made by the cut connection',
    );
    await waitUntil(
      () => proxy.sent(kSubscribe).where((f) => !f.dropped).length >= 2,
      reason: 'subscription sent again',
    );
    await settle();
    await mosqPub(broker.port, 'resub/t', 'after', qos: 1);
    await inbox.waitFor(1);
    expect(inbox.payloads, ['after']);
    await c.close();
  });
}
