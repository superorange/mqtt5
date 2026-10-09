/// Regression tests for the defects fixed after the real-broker review. Each
/// runs against a real broker; the fault proxy only shapes, drops, rewrites or
/// cuts traffic.
@Tags(['real-broker'])
@Timeout(Duration(seconds: 90))
library;

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'auth_test.dart' show ProofAuthenticator;
import 'support/common.dart';
import 'support/emqx.dart';

const _sei = Duration(seconds: 300);

/// Wraps a real TCP transport and lets a test break parts of it.
final class FlakyTransport implements MqttTransport {
  FlakyTransport(int port)
      : _inner = TcpTransport(host: '127.0.0.1', port: port);
  final TcpTransport _inner;
  bool failWrites = false;
  bool throwOnClose = false;
  bool throwOnCancel = false;
  late final StreamController<Uint8List> _incoming = StreamController(
    onCancel: () {
      _forward?.cancel();
      if (throwOnCancel) throw StateError('cancel failed');
    },
  );
  StreamSubscription<Uint8List>? _forward;

  @override
  Stream<Uint8List> get incoming => _incoming.stream;

  @override
  Future<void> connect() async {
    await _inner.connect();
    _forward = _inner.incoming.listen(_incoming.add,
        onError: _incoming.addError, onDone: _incoming.close);
  }

  @override
  void add(Uint8List data) {
    if (failWrites) throw StateError('write side is dead');
    _inner.add(data);
  }

  @override
  Future<void> flush() => _inner.flush();

  @override
  Future<void> close() async {
    await _inner.close();
    if (throwOnClose) throw StateError('close failed');
  }

  @override
  bool get isConnected => _inner.isConnected;
}

/// Verifies the TEST-CR server signature ("server-final").
final class VerifyingAuthenticator
    implements MqttAuthenticator, MqttAuthenticationVerifier {
  VerifyingAuthenticator({this.expect = 'server-final'});
  final String expect;
  final List<String?> verified = [];
  final _proof = ProofAuthenticator();

  @override
  Future<MqttAuthResponse?> authenticate(MqttAuthChallenge challenge) =>
      _proof.authenticate(challenge);

  @override
  Future<void> verifyServer(MqttAuthChallenge outcome) async {
    final got = outcome.data == null ? null : utf8.decode(outcome.data!);
    verified.add(got);
    if (got != expect) throw StateError('bad server signature $got');
  }
}

final class ThrowingAuthenticator implements MqttAuthenticator {
  ThrowingAuthenticator({this.after = 0});
  final int after;
  int calls = 0;
  @override
  Future<MqttAuthResponse?> authenticate(MqttAuthChallenge c) async {
    if (calls++ >= after) throw StateError('authenticator crashed');
    return MqttAuthResponse(
        Uint8List.fromList(utf8.encode('proof:${utf8.decode(c.data!)}')));
  }
}

void main() {
  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
    return;
  }

  late Mosquitto broker;
  late FaultProxy proxy;
  Future<void> start(
      {List<String> config = const [], bool persistence = false}) async {
    broker = await Mosquitto.start(config: config, persistence: persistence);
    proxy = await FaultProxy.start(broker.port);
  }

  tearDown(() async {
    await proxy.close();
    await broker.dispose();
  });

  group('keep alive', () {
    test(
        'MQTT-3.1.2-20: PINGREQ keeps flowing every keep alive while a '
        'PINGRESP is delayed, so the broker never drops the client', () async {
      await start();
      final c = newClient(proxy.port,
          clientId: 'ka20', pingResponseTimeout: const Duration(seconds: 10));
      await c.connect(keepAlive: const Duration(seconds: 1));
      proxy.s2c.paused = true;
      await settle(4000);
      proxy.s2c.paused = false;
      await settle(500);
      expect(broker.countLog('Received PINGREQ from ka20'),
          greaterThanOrEqualTo(3));
      expect(broker.log, isNot(contains('exceeded timeout')));
      expect(proxy.connections, 1);
      expect(c.state, MqttConnectionState.connected);
      await c.close();
    });
  });

  group('reconnect pacing', () {
    test(
        'a connection that lasted stableAfter is re-established at once; a '
        'short-lived one waits for the backoff', () async {
      await start();
      final c = MqttClient(
        host: '127.0.0.1',
        port: proxy.port,
        clientId: 'pace',
        reconnectManager: ReconnectManager(
          initialDelay: const Duration(seconds: 2),
          maxDelay: const Duration(seconds: 2),
          jitterFactor: 0,
          stableAfter: const Duration(milliseconds: 300),
        ),
      );
      await c.connect();
      await settle(500);
      var sw = Stopwatch()..start();
      proxy.cutAll();
      await waitUntil(() =>
          proxy.connections >= 2 && c.state == MqttConnectionState.connected);
      expect(sw.elapsedMilliseconds, lessThan(1000), reason: 'stable: at once');
      sw = Stopwatch()..start();
      proxy.cutAll();
      await waitUntil(() =>
          proxy.connections >= 3 && c.state == MqttConnectionState.connected);
      expect(sw.elapsedMilliseconds, greaterThanOrEqualTo(1900),
          reason: 'flapping: backoff');
      await c.close();
    });

    test(
        'reauthenticate() from a listener reacting to reconnecting is '
        'refused as not connected', () async {
      await start();
      final c = newClient(proxy.port, clientId: 'reauth-rec');
      Object? error;
      c.stateStream.listen((s) {
        if (s == MqttConnectionState.reconnecting && error == null) {
          c.reauthenticate().then((_) {}, onError: (Object e) {
            error = e;
          });
        }
      });
      await c.connect();
      proxy.cutAll();
      await waitUntil(() => error != null);
      expect(error, isA<MqttConnectionException>());
      await c.close();
    });
  });

  group('publish ordering and lifetime', () {
    test(
        'publications waiting for Receive Maximum keep call order across a '
        'lost connection and all complete', () async {
      await start(config: ['max_inflight_messages 2']);
      final c = newClient(proxy.port, clientId: 'fifo');
      await c.connect(sessionExpiryInterval: _sei);
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
      final futures = [
        for (var i = 0; i < 10; i++)
          c.publish('fifo/t', bytes('$i'), qos: MqttQos.atLeastOnce),
      ];
      await waitUntil(() => proxy.sent(kPublish).length == 2);
      await settle(100);
      expect(proxy.sent(kPublish), hasLength(2), reason: 'quota of 2');
      proxy.dropWhen = null;
      proxy.cutAll();
      final results =
          await Future.wait(futures).timeout(const Duration(seconds: 10));
      expect(
          results.every((r) => r.reasonCode != MqttReasonCode.unspecifiedError),
          isTrue);
      final firstSent = <String>[];
      for (final f in proxy.sent(kPublish)) {
        final t = text(f.publish.payload);
        if (!firstSent.contains(t)) firstSent.add(t);
      }
      expect(firstSent, [for (var i = 0; i < 10; i++) '$i']);
      await c.close();
    });

    test(
        'a publication too large for the server it is re-sent to is '
        'discarded (MQTT-3.1.2-25) and the rest of the backlog still goes',
        () async {
      await start(persistence: true);
      final c = newClient(proxy.port, clientId: 'toolarge');
      await c.connect(sessionExpiryInterval: _sei);
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
      final small1 =
          c.publish('tl/t', Uint8List(100), qos: MqttQos.atLeastOnce);
      final big = c
          .publish('tl/t', Uint8List(3000), qos: MqttQos.atLeastOnce)
          .then<Object>((r) => r, onError: (Object e) => e);
      final small2 =
          c.publish('tl/t', Uint8List(100), qos: MqttQos.exactlyOnce);
      final big2 = c
          .publish('tl/t', Uint8List(3000), qos: MqttQos.exactlyOnce)
          .then<Object>((r) => r, onError: (Object e) => e);
      proxy.dropWhen =
          (f) => f.dir == Dir.s2c && (f.type == kPuback || f.type == kPubrec);
      await waitUntil(() => proxy.sent(kPublish).length == 4);
      await broker.stop();
      proxy.dropWhen = null;
      broker = await Mosquitto.start(
          port: broker.port,
          dir: broker.dir,
          persistence: true,
          config: ['max_packet_size 1000']);
      expect((await small1.timeout(const Duration(seconds: 10))).reasonCode,
          isNotNull);
      expect(await big, isA<MqttPacketTooLargeException>());
      expect(await big2, isA<MqttPacketTooLargeException>());
      expect((await small2.timeout(const Duration(seconds: 10))).reasonCode,
          isNotNull);
      expect(c.inflightCount, 0);
      await c.close();
    });

    test(
        'a retransmission never carries the Topic Alias of the connection '
        'it was first sent on', () async {
      await start(config: ['max_topic_alias 5']);
      final c = newClient(proxy.port, clientId: 'alias-resend');
      await c.connect(sessionExpiryInterval: _sei);
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
      final r = c.publish('ar/t', bytes('x'),
          qos: MqttQos.atLeastOnce, properties: const [TopicAlias(5)]);
      await proxy.next((f) => f.dropped && f.type == kPuback);
      proxy.dropWhen = null;
      proxy.cutAll();
      await r.timeout(const Duration(seconds: 10));
      final pubs = proxy.sent(kPublish).toList();
      expect(prop(pubs.first.publish.properties, 0x23), 5);
      expect(pubs.last.dup, isTrue);
      expect(pubs.last.publish.topic, 'ar/t');
      expect(prop(pubs.last.publish.properties, 0x23), isNull);
      await c.close();
    });

    test(
        'PUBACK 0x91 to a first transmission: re-published under a new '
        'identifier, the old one stays reserved', () async {
      await start();
      var rewritten = false;
      proxy.rewrite = (f) {
        if (f.type == kPuback && !rewritten) {
          rewritten = true;
          return [
            Uint8List.fromList([0x40, 0x03, f.bytes[2], f.bytes[3], 0x91])
          ];
        }
        return null;
      };
      final c = newClient(proxy.port, clientId: 'inuse1');
      await c.connect();
      final r = await c.publish('iu/t', bytes('x'), qos: MqttQos.atLeastOnce);
      expect(r.reasonCode,
          anyOf(MqttReasonCode.success, MqttReasonCode.noMatchingSubscribers));
      final ids = proxy.sent(kPublish).map((f) => f.packetId).toList();
      expect(ids, hasLength(2));
      expect(ids[1], isNot(ids[0]));
      // The refused identifier is not handed out again in this session.
      final next =
          await c.publish('iu/t', bytes('y'), qos: MqttQos.atLeastOnce);
      expect(next.reasonCode, isNotNull);
      expect(proxy.sent(kPublish).last.packetId, isNot(ids[0]));
      await c.close();
    });

    test(
        'disconnect() with a session that outlives it keeps QoS 1 '
        'publications, which complete after connect(cleanStart: false)',
        () async {
      await start();
      final c = newClient(proxy.port, clientId: 'keep1');
      await c.connect(sessionExpiryInterval: _sei);
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
      final r = c.publish('k/t', bytes('x'), qos: MqttQos.atLeastOnce);
      await proxy.next((f) => f.dropped && f.type == kPuback);
      await c.disconnect();
      proxy.dropWhen = null;
      expect(c.inflightCount, 1);
      await c.connect(cleanStart: false, sessionExpiryInterval: _sei);
      expect(
          (await r.timeout(const Duration(seconds: 10))).reasonCode, isNotNull);
      expect(proxy.sent(kPublish).last.dup, isTrue);
      await c.close();
    });

    for (final (label, props, connackSei) in [
      (
        'DISCONNECT sets Session Expiry 0',
        const [SessionExpiryInterval(0)],
        null
      ),
      ('the broker granted Session Expiry 0', const <MqttProperty>[], 0),
    ]) {
      test('disconnect() fails pending publications when $label', () async {
        await start();
        if (connackSei != null) {
          proxy.rewrite = (f) => f.type == kConnack
              ? [
                  Uint8List.fromList(rawPacket(0x20, [
                    0,
                    0,
                    ...rawProps([0x11, 0, 0, 0, connackSei])
                  ]))
                ]
              : null;
        }
        final c = newClient(proxy.port, clientId: 'nokeep${props.length}');
        await c.connect(sessionExpiryInterval: _sei);
        proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
        final r = c.publish('k/t', bytes('x'), qos: MqttQos.atLeastOnce);
        await proxy.next((f) => f.dropped && f.type == kPuback);
        await c.disconnect(properties: props);
        await expectLater(r, throwsA(isA<MqttConnectionException>()));
        expect(c.inflightCount, 0);
        await c.close();
      });
    }

    test('close() rejects a server-only reason code and still works after',
        () async {
      await start();
      final c = newClient(proxy.port, clientId: 'closerc');
      await c.connect();
      await expectLater(
          c.close(reasonCode: MqttReasonCode.serverBusy), throwsArgumentError);
      expect(c.state, MqttConnectionState.connected);
      await c.close();
      expect(c.state, MqttConnectionState.disconnected);
    });
  });

  group('inbound delivery without a listener', () {
    test(
        'QoS 1/2 are not acknowledged until delivered; the broker is held '
        'back by Receive Maximum meanwhile', () async {
      await start();
      final c = newClient(proxy.port, clientId: 'hold');
      await c.connect(receiveMaximum: 3);
      await c.subscribe('hold/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      final pub = newClient(broker.port, clientId: 'hold-pub');
      await pub.connect();
      for (var i = 0; i < 10; i++) {
        await pub.publish('hold/t', bytes('$i'),
            qos: i.isEven ? MqttQos.atLeastOnce : MqttQos.exactlyOnce);
      }
      await settle(500);
      expect(proxy.received(kPublish), hasLength(3), reason: 'held back');
      expect(proxy.sent(kPuback), isEmpty);
      expect(proxy.sent(kPubrec), isEmpty);
      final inbox = Inbox(c);
      await inbox.waitFor(10);
      expect(inbox.payloads, [for (var i = 0; i < 10; i++) '$i']);
      await waitUntil(() => proxy.sent(kPubcomp).length == 5);
      await pub.close();
      await c.close();
    });

    test('QoS 0 backlog is capped at 1000, oldest dropped', () async {
      await start();
      final log = CollectingLogger();
      final c = newClient(proxy.port, clientId: 'cap', logger: log);
      await c.connect();
      await c.subscribe('cap/t');
      final pub = newClient(broker.port, clientId: 'cap-pub');
      await pub.connect();
      for (var i = 0; i < 1100; i++) {
        await pub.publish('cap/t', bytes('$i'));
      }
      await waitUntil(() => proxy.received(kPublish).length == 1100);
      final inbox = Inbox(c);
      await inbox.waitFor(1000);
      await settle(200);
      expect(inbox.messages, hasLength(1000));
      expect(inbox.payloads.first, '100');
      expect(
          log.lines.where((l) => l.contains('dropped the oldest')), isNotEmpty);
      await pub.close();
      await c.close();
    });

    test(
        'a backlog held across a lost connection: acknowledgements are not '
        'sent on a dead or later connection; the broker re-sends and the '
        'messages still arrive', () async {
      await start();
      final log = CollectingLogger();
      final c = newClient(proxy.port, clientId: 'stale', logger: log);
      await c.connect(sessionExpiryInterval: _sei);
      await c.subscribe('st/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce));
      final pub = newClient(broker.port, clientId: 'stale-pub');
      await pub.connect();
      await pub.publish('st/t', bytes('a'), qos: MqttQos.atLeastOnce);
      await pub.publish('st/t', bytes('b'), qos: MqttQos.atLeastOnce);
      await waitUntil(() => proxy.received(kPublish).length == 2);
      // Lose the connection and keep it down, then start listening.
      proxy.refuse = true;
      proxy.cutAll();
      await waitUntil(() => c.state != MqttConnectionState.connected);
      final inbox = Inbox(c);
      await inbox.waitFor(2);
      expect(
          log.lines.where((l) => l.contains('left for the broker to re-send')),
          hasLength(2));
      // Now reconnect: the broker re-sends both (DUP) and they are acked.
      proxy.refuse = false;
      await waitUntil(() => c.state == MqttConnectionState.connected);
      await inbox.waitFor(4);
      expect(inbox.messages.skip(2).every((m) => m.duplicate), isTrue);
      await waitUntil(
          () => proxy.sent(kPuback).where((f) => f.connection > 1).length == 2);
      await pub.close();
      await c.close();
    });

    test(
        'the same backlog delivered only after the reconnect: stale '
        'acknowledgements are skipped', () async {
      await start();
      final c = newClient(proxy.port, clientId: 'stale2');
      await c.connect(sessionExpiryInterval: _sei);
      await c.subscribe('st2/t',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
      final pub = newClient(broker.port, clientId: 'stale2-pub');
      await pub.connect();
      await pub.publish('st2/t', bytes('a'), qos: MqttQos.exactlyOnce);
      await waitUntil(() => proxy.received(kPublish).length == 1);
      proxy.cutAll();
      await waitUntil(() =>
          proxy.connections >= 2 && c.state == MqttConnectionState.connected);
      await waitUntil(() => proxy.received(kPublish).length == 2);
      final inbox = Inbox(c);
      await inbox.waitFor(1);
      await settle(300);
      expect(inbox.payloads, ['a'], reason: 'QoS 2 delivered once');
      await waitUntil(() => proxy.sent(kPubcomp).isNotEmpty);
      expect(proxy.sent(kPubrec).every((f) => f.connection >= 2), isTrue);
      await pub.close();
      await c.close();
    });
  });

  group('protocol checks added', () {
    test(
        'Request Problem Information 0: a Reason String in PUBACK is a '
        'protocol error; mosquitto itself complies', () async {
      await start();
      final c = newClient(proxy.port, clientId: 'rpi0');
      await c.connect(properties: const [RequestProblemInformation(0)]);
      await c.publish('rpi/t', bytes('x'), qos: MqttQos.atLeastOnce);
      expect(proxy.sent(kDisconnect), isEmpty);
      proxy.injectToClient(rawPacket(0x40, [
        0x0F,
        0xFF,
        0x00,
        ...rawProps([0x1F, ...rawString('why')]),
      ]));
      final d = await proxy.next(
          (f) => f.dir == Dir.c2s && f.type == kDisconnect,
          includeHistory: true);
      expect(d.ackReasonCode, 0x82);
      await c.close();
    });

    for (final (name, first) in [('SUBACK', 0x90), ('UNSUBACK', 0xB0)]) {
      test(
          'Request Problem Information 0: a Reason String in $name is a '
          'protocol error', () async {
        await start();
        final c = newClient(proxy.port, clientId: 'rpi-$name');
        await c.connect(properties: const [RequestProblemInformation(0)]);
        proxy.injectToClient(rawPacket(first, [
          0x0F,
          0xFF,
          ...rawProps([0x1F, ...rawString('why')]),
          0x00,
        ]));
        final d = await proxy.next(
            (f) => f.dir == Dir.c2s && f.type == kDisconnect,
            includeHistory: true);
        expect(d.ackReasonCode, 0x82);
        await c.close();
      });
    }

    test(
        'Request Problem Information 0: a User Property in AUTH is a '
        'protocol error', () async {
      await start();
      final c = newClient(proxy.port, clientId: 'rpi-auth');
      await c.connect(properties: const [RequestProblemInformation(0)]);
      proxy.injectToClient(rawPacket(0xF0, [
        0x18,
        ...rawProps([0x26, ...rawString('k'), ...rawString('v')]),
      ]));
      final d = await proxy.next(
          (f) => f.dir == Dir.c2s && f.type == kDisconnect,
          includeHistory: true);
      expect(d.ackReasonCode, 0x82);
      await c.close();
    });

    test('Request Problem Information 1 (default): Reason String accepted',
        () async {
      await start();
      final c = newClient(proxy.port, clientId: 'rpi1');
      await c.connect();
      proxy.injectToClient(rawPacket(0x40, [
        0x0F,
        0xFF,
        0x00,
        ...rawProps([0x1F, ...rawString('why')]),
      ]));
      await settle(300);
      expect(proxy.sent(kDisconnect), isEmpty);
      await c.close();
    });
  });

  group('enhanced authentication (real plugin)', () {
    late String plugin;
    setUp(() async {
      plugin = await buildAuthPlugin();
      broker = await Mosquitto.start(config: ['plugin $plugin']);
      proxy = await FaultProxy.start(broker.port);
    });

    test(
        'verifier accepts the server signature on connect and on '
        're-authentication', () async {
      final auth = VerifyingAuthenticator();
      final c = newClient(proxy.port, clientId: 'v1', authenticator: auth);
      await c.connect(
          authenticationMethod: 'TEST-CR',
          authenticationData: bytes('client-first'));
      await c.reauthenticate(authenticationData: bytes('client-first-reauth'));
      expect(auth.verified, ['server-final', 'server-final']);
      await c.close();
    });

    test('verifier rejecting the server closes the connection with 0x80',
        () async {
      final c = newClient(proxy.port,
          clientId: 'v2', authenticator: VerifyingAuthenticator(expect: 'x'));
      await expectLater(
          c.connect(
              authenticationMethod: 'TEST-CR',
              authenticationData: bytes('client-first')),
          throwsA(isA<MqttAuthenticationException>()));
      await settle(200);
      expect(proxy.sent(kDisconnect).single.ackReasonCode, 0x80);
      expect(proxy.sent(kConnect), hasLength(1));
      await c.close();
    });

    test('a CONNACK naming another Authentication Method is refused', () async {
      proxy.rewrite = (f) => f.type == kConnack
          ? [
              Uint8List.fromList(rawPacket(0x20, [
                0,
                0,
                ...rawProps([0x15, ...rawString('OTHER')])
              ]))
            ]
          : null;
      final c = newClient(proxy.port, clientId: 'v3');
      await expectLater(
          c.connect(
              authenticationMethod: 'TEST-ONE',
              authenticationData: bytes('ok')),
          throwsA(isA<MqttProtocolException>()));
      await c.close();
    });

    test('an authenticator that throws fails the handshake cleanly', () async {
      final c = newClient(proxy.port,
          clientId: 'v4', authenticator: ThrowingAuthenticator());
      await expectLater(
          c.connect(
              authenticationMethod: 'TEST-CR',
              authenticationData: bytes('client-first')),
          throwsA(isA<MqttAuthenticationException>()));
      expect(c.state, MqttConnectionState.disconnected);
      await c.close();
    });

    test(
        'an authenticator that throws during re-authentication closes the '
        'connection and the call fails', () async {
      final c = newClient(proxy.port,
          clientId: 'v5', authenticator: ThrowingAuthenticator(after: 1));
      await c.connect(
          authenticationMethod: 'TEST-CR',
          authenticationData: bytes('client-first'));
      await expectLater(
          c.reauthenticate(authenticationData: bytes('client-first-reauth')),
          throwsA(isA<MqttAuthenticationException>()));
      await waitUntil(() => c.state == MqttConnectionState.disconnected);
      await c.close();
    });

    test(
        'a server-initiated re-authentication whose challenge cannot be '
        'answered closes the connection', () async {
      final c = newClient(proxy.port,
          clientId: 'v6', authenticator: ThrowingAuthenticator(after: 1));
      final errors = <Object>[];
      c.errors.listen(errors.add);
      await c.connect(
          authenticationMethod: 'TEST-CR',
          authenticationData: bytes('client-first'));
      // The server starts an exchange on its own (AUTH 0x18 with a
      // challenge); nobody is waiting in reauthenticate().
      proxy.injectToClient(rawPacket(0xF0, [
        0x18,
        ...rawProps([
          0x15,
          ...rawString('TEST-CR'),
          0x16,
          ...rawString('server-challenge'),
        ]),
      ]));
      await waitUntil(() => c.state == MqttConnectionState.disconnected);
      await waitUntil(() => errors.isNotEmpty);
      expect(errors.single, isA<MqttAuthenticationException>());
      await c.close();
    });
  });

  group('custom transports', () {
    test(
        'a transport whose close() and cancel throw does not break '
        'teardown or reconnects', () async {
      await start();
      final transports = <FlakyTransport>[];
      final log = CollectingLogger();
      final c = MqttClient(
        host: 'unused',
        clientId: 'flaky-close',
        logger: log,
        reconnectManager: fastReconnect(),
        transportFactory: () {
          final t = FlakyTransport(proxy.port)
            ..throwOnClose = true
            ..throwOnCancel = true;
          transports.add(t);
          return t;
        },
      );
      await c.connect();
      proxy.cutAll();
      await waitUntil(() =>
          transports.length >= 2 && c.state == MqttConnectionState.connected);
      expect(log.lines.where((l) => l.contains('Transport close failed')),
          isNotEmpty);
      expect(log.lines.where((l) => l.contains('listener cancel failed')),
          isNotEmpty);
      await c.close();
      expect(c.state, MqttConnectionState.disconnected);
    });

    test(
        'a transport that can no longer write is detected at the next '
        'PINGREQ and replaced', () async {
      await start();
      final transports = <FlakyTransport>[];
      final c = MqttClient(
        host: 'unused',
        clientId: 'flaky-write',
        reconnectManager: fastReconnect(),
        transportFactory: () {
          final t = FlakyTransport(proxy.port);
          transports.add(t);
          return t;
        },
      );
      await c.connect(keepAlive: const Duration(seconds: 1));
      transports.first.failWrites = true;
      await waitUntil(
          () =>
              transports.length >= 2 &&
              c.state == MqttConnectionState.connected,
          timeout: const Duration(seconds: 5));
      await c.close();
    });

    test('TcpTransport used directly: add before connect fails, flush works',
        () async {
      await start();
      final t = TcpTransport(host: '127.0.0.1', port: broker.port);
      expect(() => t.add(Uint8List(1)), throwsA(isA<MqttTransportException>()));
      await t.connect();
      final got = t.incoming.first;
      t.add(MqttPacketCodec.encode(MqttConnectPacket(clientId: 'raw')));
      await t.flush();
      final connack =
          MqttPacketCodec.decode(await got.timeout(const Duration(seconds: 5)));
      expect(connack, isA<MqttConnackPacket>());
      await t.close();
    });
  });

  group('state events', () {
    test('no state change is published after close()', () async {
      await start();
      final c = newClient(proxy.port, clientId: 'quiet');
      final states = <MqttConnectionState>[];
      c.stateStream.listen(states.add);
      await c.connect();
      await c.close();
      final count = states.length;
      proxy.cutAll();
      await settle(300);
      expect(states, hasLength(count));
      expect(states.last, MqttConnectionState.disconnected);
    });
  });

  group('EMQX recovery semantics', () {
    test(
        'PUBCOMP 0x92 to a re-sent PUBREL and PUBREC 0x91 to a DUP are '
        'completions, not failures', () async {
      if (!await Emqx.available) return markTestSkipped('no EMQX image');
      final emqx = await Emqx.start();
      addTearDown(emqx.dispose);
      broker = await Mosquitto.start(); // satisfies tearDown
      proxy = await FaultProxy.start(emqx.port);
      final c = newClient(proxy.port, clientId: 'emqx-rec');
      await c.connect(sessionExpiryInterval: _sei);

      // PUBCOMP lost -> PUBREL re-sent on the next connection.
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPubcomp;
      final a = c.publish('er/t', bytes('a'), qos: MqttQos.exactlyOnce);
      await proxy.next((f) => f.dropped && f.type == kPubcomp);
      proxy.dropWhen = null;
      proxy.cutAll();
      expect((await a.timeout(const Duration(seconds: 10))).reasonCode,
          MqttReasonCode.success);

      // PUBREC lost -> PUBLISH re-sent with DUP -> EMQX answers 0x91.
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPubrec;
      final b = c.publish('er/t', bytes('b'), qos: MqttQos.exactlyOnce);
      await proxy.next((f) => f.dropped && f.type == kPubrec);
      proxy.dropWhen = null;
      proxy.cutAll();
      expect((await b.timeout(const Duration(seconds: 10))).reasonCode,
          MqttReasonCode.success);
      printOnFailure(proxy.dump());
      await c.close();
    });
  });
}
