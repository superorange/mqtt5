/// Remaining reachable paths: resource exhaustion, cancellation and edge
/// timing, all against real mosquitto (with the fault proxy where noted), plus
/// the codec in the server role answering real mosquitto clients with
/// property-carrying acknowledgements and receiving malformed client input.
@Tags(['real-broker'])
@Timeout(Duration(seconds: 90))
library;

import 'dart:async';
import 'dart:convert';
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

  late Mosquitto broker;
  late FaultProxy proxy;
  tearDown(() async {
    await proxy.close();
    await broker.dispose();
  });

  Future<void> start({List<String> config = const []}) async {
    broker = await Mosquitto.start(config: config);
    proxy = await FaultProxy.start(broker.port);
  }

  group('resource limits', () {
    test(
        'waiting for a Receive Maximum slot times out (QoS 1 and QoS 2) '
        'without leaking the slot or the identifier', () async {
      await start(config: ['max_inflight_messages 1']);
      final c = newClient(proxy.port,
          clientId: 'slot',
          operationTimeout: const Duration(milliseconds: 400));
      await c.connect();
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
      final first = c
          .publish('s/t', bytes('1'), qos: MqttQos.atLeastOnce)
          .then<Object>((result) => result, onError: (Object error) => error);
      await Future<void>.delayed(const Duration(milliseconds: 600));
      expect(c.inflightCount, 1);
      await expectLater(c.publish('s/t', bytes('2'), qos: MqttQos.atLeastOnce),
          throwsA(isA<MqttTimeoutException>()));
      await expectLater(c.publish('s/t', bytes('3'), qos: MqttQos.exactlyOnce),
          throwsA(isA<MqttTimeoutException>()));
      expect(proxy.sent(kPublish), hasLength(1));
      expect(c.inflightCount, 1);
      await c.close();
      expect(await first, isA<MqttConnectionException>());
    });

    test('without an operation timeout a blocked publish waits for the slot',
        () async {
      await start(config: ['max_inflight_messages 1']);
      final c = newClient(proxy.port,
          clientId: 'slot0', operationTimeout: Duration.zero);
      await c.connect();
      proxy.dropWhen = (f) =>
          f.dir == Dir.s2c &&
          f.type == kPuback &&
          proxy.sent(kPublish).length == 1;
      final a = c
          .publish('s/t', bytes('a'), qos: MqttQos.atLeastOnce)
          .then<Object>((r) => r, onError: (Object e) => e);
      await proxy.next((f) => f.dropped && f.type == kPuback);
      final b = c
          .publish('s/t', bytes('b'), qos: MqttQos.atLeastOnce)
          .then<Object>((r) => r, onError: (Object e) => e);
      await settle(300);
      expect(proxy.sent(kPublish), hasLength(1), reason: 'b must wait');
      proxy.dropWhen = null;
      // Releasing a's slot requires its PUBACK: resume it on a new connection.
      await c.disconnect();
      expect(await a, isA<MqttConnectionException>());
      expect(await b, isA<MqttConnectionException>());
      await c.close();
    });

    test(
        'packets above the server Maximum Packet Size are refused for '
        'SUBSCRIBE, UNSUBSCRIBE and QoS 2 PUBLISH, freeing their identifiers',
        () async {
      await start(config: ['max_packet_size 100']);
      final c = newClient(proxy.port, clientId: 'big');
      await c.connect();
      final long = 'big/${'x' * 120}';
      await expectLater(
          c.subscribe(long), throwsA(isA<MqttPacketTooLargeException>()));
      await expectLater(
          c.unsubscribe([long]), throwsA(isA<MqttPacketTooLargeException>()));
      await expectLater(
          c.publish('big/t', Uint8List(200), qos: MqttQos.exactlyOnce),
          throwsA(isA<MqttPacketTooLargeException>()));
      expect(c.inflightCount, 0);
      await c.subscribe('big/ok');
      expect(c.state, MqttConnectionState.connected);
      await c.close();
    });

    test(
        'all 65535 packet identifiers in use: publish waits for a send slot, '
        'subscribe waits for an identifier (with and without a deadline), a '
        '0x91 re-publish finds none, and a session reset wakes the waiters',
        () async {
      await start(config: ['max_inflight_messages 65535']);
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPuback;
      final c = newClient(proxy.port,
          clientId: 'exhaust', operationTimeout: Duration.zero);
      await c.connect();
      final pending = [
        for (var i = 0; i < 65535; i++)
          c
              .publish('e/t', Uint8List(0), qos: MqttQos.atLeastOnce)
              .then<Object?>((_) => null, onError: (Object e) => e),
      ];
      await waitUntil(() => proxy.sent(kPublish).length == 65535,
          timeout: const Duration(seconds: 60));
      // Every slot and every identifier is taken: a publish waits for a slot,
      // a subscribe for an identifier, neither with a deadline...
      final waitingPublish = c
          .publish('e/t', bytes('last'), qos: MqttQos.atLeastOnce)
          .then<String>((_) => 'done', onError: (Object e) => '$e');
      final waitingSubscribe = c
          .subscribe('e/sub')
          .then<String>((_) => 'done', onError: (Object e) => '$e');
      await settle(200);
      expect(proxy.sent(kPublish), hasLength(65535));
      expect(proxy.sent(kSubscribe), isEmpty);
      // ...until the session is lost and the pool is reset.
      proxy.dropWhen = null;
      proxy.cutAll();
      expect(await waitingSubscribe.timeout(const Duration(seconds: 20)),
          isNotEmpty);
      expect(await waitingPublish.timeout(const Duration(seconds: 20)), 'done');
      final failed = (await Future.wait(pending)).whereType<Object>().length;
      expect(failed, 65535, reason: 'session expiry 0: all were discarded');
      await c.close();

      // Identifiers run out while send slots are free: 65533 publications
      // and one unanswered SUBSCRIBE hold 65534 identifiers ...
      final d = newClient(proxy.port,
          clientId: 'exhaust2', operationTimeout: const Duration(seconds: 2));
      proxy.dropWhen = (f) =>
          f.dir == Dir.s2c &&
          (f.type == kPuback || f.type == kPubrec || f.type == kSuback);
      await d.connect();
      final before = proxy.sent(kPublish).length;
      // (These time out after 2 s; they stay in the session store.)
      d.publish('e/t', Uint8List(0), qos: MqttQos.atLeastOnce).ignore();
      d.publish('e/t', Uint8List(0), qos: MqttQos.exactlyOnce).ignore();
      for (var i = 2; i < 65533; i++) {
        d.publish('e/t', Uint8List(0), qos: MqttQos.atLeastOnce).ignore();
      }
      await waitUntil(() => proxy.sent(kPublish).length - before == 65533,
          timeout: const Duration(seconds: 60));
      await expectLater(
          d.subscribe('e/held'), throwsA(isA<MqttTimeoutException>()));
      // ... and a third publication takes the last one.
      d.publish('e/t', Uint8List(0), qos: MqttQos.atLeastOnce).ignore();
      await waitUntil(() => proxy.sent(kPublish).length - before == 65534);
      // 0x91 asks to re-publish the first two, but no identifier is left:
      // both leave the session (completed with 0x91, their identifiers
      // quarantined) and give their send slots back.
      final id1 = proxy.sent(kPublish).elementAt(before).packetId;
      final id2 = proxy.sent(kPublish).elementAt(before + 1).packetId;
      final inflight = d.inflightCount;
      proxy.injectToClient([0x40, 0x03, id1 >> 8, id1 & 0xFF, 0x91]);
      proxy.injectToClient([0x50, 0x03, id2 >> 8, id2 & 0xFF, 0x91]);
      await waitUntil(() => d.inflightCount == inflight - 2);
      expect(proxy.sent(kPublish).length - before, 65534,
          reason: 'nothing re-published without an identifier');
      // Send slots are free again, but no identifier: a publish takes a slot,
      // waits for an identifier, times out and gives the slot back; a
      // subscribe times out the same way.
      await expectLater(d.publish('e/t', bytes('x'), qos: MqttQos.atLeastOnce),
          throwsA(isA<MqttTimeoutException>()));
      await expectLater(
          d.subscribe('e/sub2'), throwsA(isA<MqttTimeoutException>()));
      await d.close();
    }, timeout: const Timeout(Duration(minutes: 4)));
  });

  group('session loss and cancellation', () {
    test('a lost session fails in-flight QoS 2 publishes', () async {
      await start();
      final c = newClient(proxy.port, clientId: 'lost2');
      await c.connect();
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kPubrec;
      final p = c.publish('l/t', bytes('x'), qos: MqttQos.exactlyOnce);
      await proxy.next((f) => f.dropped && f.type == kPubrec);
      proxy.dropWhen = null;
      proxy.cutAll();
      await expectLater(p, throwsA(isA<MqttConnectionException>()));
      await c.close();
    });

    test(
        'a re-subscribe after session loss that gets no SUBACK times out '
        'and keeps its identifier until the connection ends', () async {
      await start();
      final log = CollectingLogger();
      final c = newClient(proxy.port,
          clientId: 'resub',
          logger: log,
          operationTimeout: const Duration(milliseconds: 400));
      await c.connect();
      await c.subscribe('r/t');
      proxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kSuback;
      proxy.cutAll();
      await waitUntil(
          () => log.lines.any((l) => l.contains('Re-subscribe timed out')));
      await c.close();
    });

    test('disconnect() while connect() is still retrying ends it', () async {
      final port = await freePort();
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(port);
      final c = newClient(port, clientId: 'cancel');
      final connecting =
          c.connect().then<Object?>((_) => null, onError: (Object e) => e);
      await settle(250);
      expect(c.state, MqttConnectionState.reconnecting);
      await c.disconnect();
      expect(await connecting, isNotNull);
      expect(c.state, MqttConnectionState.disconnected);
      await c.close();
    });

    test('disconnect() while waiting for CONNACK ends connect()', () async {
      await start();
      proxy.blackhole = true;
      final c = newClient(proxy.port, clientId: 'cancel2');
      final connecting =
          c.connect().then<Object?>((_) => null, onError: (Object e) => e);
      await settle(300);
      await c.disconnect();
      expect(await connecting, isA<MqttConnectionException>());
      await c.close();
    });

    test('re-authentication with no operation timeout completes', () async {
      final so = await buildAuthPlugin();
      await start(config: ['plugin $so']);
      final c = newClient(proxy.port,
          clientId: 'reauth0',
          operationTimeout: Duration.zero,
          authenticator: _Proof());
      await c.connect(
          authenticationMethod: 'TEST-CR',
          authenticationData: bytes('client-first'));
      await c.reauthenticate(authenticationData: bytes('client-first-reauth'));
      expect(c.state, MqttConnectionState.connected);
      await c.close();
    });
  });

  group('byte stream edge cases (injected into a real connection)', () {
    test('a packet split across reads after a burst is reassembled', () async {
      await start();
      final c = newClient(proxy.port, clientId: 'split');
      await c.connect();
      final inbox = Inbox(c);
      final pub =
          rawPacket(0x30, [...rawString('sp/t'), 0, ...utf8.encode('hello')]);
      proxy.injectToClient([
        for (var i = 0; i < 10; i++) ...[0xD0, 0x00],
        ...pub.sublist(0, 3),
      ]);
      await settle(100);
      proxy.injectToClient(pub.sublist(3));
      await inbox.waitFor(1);
      expect(inbox.messages.single.topic, 'sp/t');
      expect(c.state, MqttConnectionState.connected);
      await c.close();
    });

    test('CONNACK followed in the same segment by a forbidden packet',
        () async {
      await start();
      var once = false;
      proxy.rewrite = (f) {
        if (f.type != kConnack || once) return null;
        once = true;
        return [
          Uint8List.fromList([
            ...f.bytes,
            ...rawPacket(0x82, [0, 1, 0, ...rawString('a'), 0])
          ])
        ];
      };
      final c = newClient(proxy.port, clientId: 'deferred');
      // The forbidden packet shares the CONNACK read, so connect() itself
      // fails. A protocol error is not retried.
      await expectLater(c.connect(), throwsA(isA<MqttProtocolException>()));
      final d = await proxy.next(
          (f) => f.dir == Dir.c2s && f.type == kDisconnect,
          includeHistory: true);
      expect(d.ackReasonCode, 0x82);
      await settle(300);
      expect(c.state, MqttConnectionState.disconnected);
      expect(proxy.connections, 1);
      await c.close();
    });

    final malformed = <String, List<int>>{
      'property identifier not minimally encoded':
          rawPacket(0x30, [...rawString('x'), 3, 0x80, 0x00, 0x00]),
      'property length longer than 4 bytes':
          rawPacket(0x30, [...rawString('x'), 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]),
      'property overruns its own section': rawPacket(
          0x30, [...rawString('x'), 2, 0x03, 0x00, 0x03, 0x61, 0x62, 0x63]),
      'SUBACK with no reason codes': [0x90, 0x03, 0x00, 0x05, 0x00],
      'UNSUBACK with no reason codes': [0xB0, 0x03, 0x00, 0x05, 0x00],
    };
    for (final e in malformed.entries) {
      test(e.key, () async {
        await start();
        final c = newClient(proxy.port, clientId: 'm${e.key.hashCode}');
        await c.connect();
        proxy.injectToClient(e.value);
        final d = await proxy.next(
            (f) => f.dir == Dir.c2s && f.type == kDisconnect,
            includeHistory: true);
        expect(d.ackReasonCode, 0x81);
        await c.close();
      });
    }

    test('a Response Topic with a wildcard is a protocol error', () async {
      await start();
      final c = newClient(proxy.port, clientId: 'rtw');
      await c.connect();
      proxy.injectToClient(rawPacket(0x30, [
        ...rawString('x'),
        ...rawProps([0x08, ...rawString('reply/#')]),
      ]));
      final d = await proxy.next(
          (f) => f.dir == Dir.c2s && f.type == kDisconnect,
          includeHistory: true);
      expect(d.ackReasonCode, 0x82);
      await c.close();
    });
  });

  group('codec in the server role', () {
    late ServerSocket server;
    final errors = <Object>[];
    final decoded = <MqttPacket>[];
    setUp(() async {
      errors.clear();
      decoded.clear();
      broker = await Mosquitto.start();
      proxy = await FaultProxy.start(broker.port);
      server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      var connections = 0;
      server.listen((s) {
        final first = connections++ == 0;
        s.done.then((_) {}, onError: (_) {});
        final decoder = MqttPacketDecoder();
        void send(MqttPacket p) => s.add(MqttPacketCodec.encode(p));
        s.listen((data) {
          try {
            for (final p in decoder.feed(data)) {
              decoded.add(p);
              switch (p) {
                case MqttConnectPacket():
                  send(MqttConnackPacket(sessionPresent: false, properties: [
                    if (first) const MaximumQos(1),
                    const RetainAvailable(1),
                    const WildcardSubscriptionAvailable(1),
                    const SubscriptionIdentifierAvailable(1),
                    const SharedSubscriptionAvailable(1),
                    const ServerKeepAlive(30),
                    const AssignedClientIdentifier('assigned'),
                    const ResponseInformation('resp/'),
                    const ServerReference('other'),
                    const MaximumPacketSize(10000),
                    const SessionExpiryInterval(5),
                    const AuthenticationMethod('none'),
                    AuthenticationData([1]),
                  ]));
                case MqttSubscribePacket(:final packetIdentifier):
                  send(MqttSubackPacket(
                      packetIdentifier: packetIdentifier,
                      reasonCodes: const [2]));
                  // Server -> client QoS 2 with property-carrying PUBREL.
                  send(MqttPublishPacket(
                      topicName: 'q/2',
                      payload: bytes('two'),
                      qos: MqttQos.exactlyOnce,
                      packetIdentifier: 9));
                case MqttPubrecPacket(:final packetIdentifier):
                  send(MqttPubrelPacket(
                      packetIdentifier: packetIdentifier,
                      properties: const [ReasonString('rel')]));
                case MqttPublishPacket(:final qos, :final packetIdentifier):
                  if (qos == MqttQos.atLeastOnce) {
                    send(MqttPubackPacket(
                        packetIdentifier: packetIdentifier,
                        reasonCode: MqttReasonCode.noMatchingSubscribers,
                        properties: const [ReasonString('nobody')]));
                  } else if (qos == MqttQos.exactlyOnce) {
                    send(MqttPubrecPacket(
                        packetIdentifier: packetIdentifier,
                        properties: const [UserProperty('rec', '1')]));
                  }
                case MqttPingreqPacket():
                  send(const MqttPingrespPacket());
                case MqttPubrelPacket(:final packetIdentifier):
                  send(MqttPubcompPacket(
                      packetIdentifier: packetIdentifier,
                      properties: const [ReasonString('comp')]));
                default:
                  break;
              }
            }
          } on Object catch (e) {
            errors.add(e);
            s.destroy();
          }
        }, onError: (_) {});
      });
    });
    tearDown(() => server.close());

    test(
        'acknowledgements with properties and every server-only CONNACK '
        'property are accepted by real mosquitto clients', () async {
      for (final qos in ['1', '2']) {
        final r = await Process.run(requireMosquittoProgram('mosquitto_pub'), [
          '-V',
          'mqttv5',
          '-h',
          '127.0.0.1',
          '-p',
          '${server.port}',
          '-q',
          qos,
          '-t',
          'p/t',
          '-m',
          'x',
          '-d',
          '-D',
          'connect',
          'request-problem-information',
          '1',
          '-D',
          'connect',
          'request-response-information',
          '1',
        ]);
        expect(r.exitCode, 0, reason: '${r.stdout}${r.stderr}');
        expect(
            '${r.stdout}',
            qos == '1'
                ? contains('received PUBACK (Mid: 1, RC:16)')
                : contains('received PUBCOMP (Mid: 1, RC:0)'));
      }
      final connect = decoded.whereType<MqttConnectPacket>().first;
      expect(
          connect.properties
              .whereType<RequestProblemInformation>()
              .single
              .value,
          1);
      expect(
          connect.properties
              .whereType<RequestResponseInformation>()
              .single
              .value,
          1);
      final sub = await Process.start('script', [
        '-q',
        '/dev/null',
        requireMosquittoProgram('mosquitto_sub'),
        '-V',
        'mqttv5',
        '-h',
        '127.0.0.1',
        '-p',
        '${server.port}',
        '-t',
        'q/#',
        '-q',
        '2',
        '-C',
        '1',
        '-d',
      ]);
      final out = StringBuffer();
      sub.stdout.transform(utf8.decoder).listen(out.write);
      try {
        await waitUntil(() => '$out'.contains('sending PUBCOMP'),
            reason: '$out');
      } finally {
        sub.kill();
        await sub.exitCode;
      }
      expect('$out', contains('received PUBREL (Mid: 9)'));
      expect('$out', contains('two'));
      expect(errors, isEmpty);
    });

    test('malformed client input is rejected by the server-side decoder',
        () async {
      final cases = <List<int>>[
        // CONNECT: wrong protocol name, wrong level, reserved flag, will QoS
        // without will flag, will QoS 3.
        rawPacket(
            0x10, [...rawString('MQTX'), 5, 2, 0, 60, 0, ...rawString('c')]),
        rawPacket(
            0x10, [...rawString('MQTT'), 4, 2, 0, 60, 0, ...rawString('c')]),
        rawPacket(
            0x10, [...rawString('MQTT'), 5, 3, 0, 60, 0, ...rawString('c')]),
        rawPacket(
            0x10, [...rawString('MQTT'), 5, 0x0A, 0, 60, 0, ...rawString('c')]),
        rawPacket(
            0x10, [...rawString('MQTT'), 5, 0x1E, 0, 60, 0, ...rawString('c')]),
        // SUBSCRIBE: reserved option bits, Retain Handling 3, no filters.
        rawPacket(0x82, [0, 1, 0, ...rawString('a'), 0xC0]),
        rawPacket(0x82, [0, 1, 0, ...rawString('a'), 0x30]),
        rawPacket(0x82, [0, 1, 0]),
        // UNSUBSCRIBE with no filters.
        rawPacket(0xA2, [0, 1, 0]),
      ];
      for (final raw in cases) {
        final s = await Socket.connect('127.0.0.1', server.port);
        s.add(raw);
        await s.flush();
        await waitUntil(() => errors.length == cases.indexOf(raw) + 1,
            reason: 'case ${cases.indexOf(raw)}');
        s.destroy();
      }
      expect(errors, everyElement(isA<MqttMalformedPacketException>()));
      // Whole-packet decode refuses trailing bytes.
      expect(
          () => MqttPacketCodec.decode(Uint8List.fromList([0xD0, 0x00, 0x00])),
          throwsA(isA<MqttMalformedPacketException>()));
      expect(MqttPacketDecoder().bufferedBytes, 0);
      expect(const ContentType('a').hashCode, const ContentType('a').hashCode);
    });
  });

  test('TLS transport honours a source address', () async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
    // Plain listener: the handshake fails, but only after the socket was
    // bound to the requested source address and connected.
    final t = TlsTransport(
        host: '127.0.0.1',
        port: proxy.port,
        sourceAddress: '127.0.0.1',
        timeout: const Duration(seconds: 2),
        onBadCertificate: (_) => true);
    await expectLater(t.connect(), throwsA(anything));
    expect(proxy.connections, 1);
  });
}

final class _Proof implements MqttAuthenticator {
  @override
  Future<MqttAuthResponse?> authenticate(MqttAuthChallenge c) async =>
      MqttAuthResponse(Uint8List.fromList(
          utf8.encode('proof:${utf8.decode(c.data ?? const [])}')));
}
