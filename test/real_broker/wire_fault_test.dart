/// Peer-violation handling. A real client talks to a real mosquitto through
/// the fault proxy, which corrupts or injects bytes on the broker -> client
/// path — the only way to see how the client reacts to a broker that breaks
/// the protocol, since a compliant broker never will. Expectations follow
/// section 4.13 and the Reason Code tables; several cover reason codes 0.4.0
/// got wrong (fixed since).
@Tags(['real-broker', 'wire-fault'])
@Timeout(Duration(seconds: 30))
library;

import 'dart:async';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'auth_test.dart' show ProofAuthenticator;
import 'support/common.dart';

List<int> _publish(String topic,
    {int qos = 0,
    int pid = 1,
    List<int> props = const [],
    int flags = 0,
    List<int> payload = const [0x78]}) {
  final first = 0x30 | flags | (qos << 1);
  return rawPacket(first, [
    ...rawString(topic),
    if (qos > 0) ...[pid >> 8, pid & 0xFF],
    ...rawProps(props),
    ...payload,
  ]);
}

void main() {
  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
    return;
  }

  late Mosquitto broker;
  setUpAll(() async => broker = await Mosquitto.start());
  tearDownAll(() => broker.dispose());

  late FaultProxy proxy;
  setUp(() async => proxy = await FaultProxy.start(broker.port));
  tearDown(() => proxy.close());

  var seq = 0;
  Future<MqttClient> connected({
    int receiveMaximum = 65535,
    int maximumPacketSize = 268435455,
    int topicAliasMaximum = 0,
    bool autoReconnect = true,
  }) async {
    final c = newClient(proxy.port,
        clientId: 'wf${seq++}', autoReconnect: autoReconnect);
    await c.connect(
        receiveMaximum: receiveMaximum,
        maximumPacketSize: maximumPacketSize,
        topicAliasMaximum: topicAliasMaximum);
    return c;
  }

  Future<int?> clientDisconnectReason() async {
    final f = await proxy.next((f) => f.dir == Dir.c2s && f.type == kDisconnect,
        includeHistory: true, timeout: const Duration(seconds: 5));
    return f.ackReasonCode;
  }

  /// Injects [raw] into an established connection and returns the reason
  /// code of the DISCONNECT the client answers with; then checks the client
  /// recovers by reconnecting.
  Future<int?> injectExpectingDisconnect(MqttClient c, List<int> raw) async {
    proxy.injectToClient(raw);
    final rc = await clientDisconnectReason();
    await waitUntil(() =>
        proxy.connections >= 2 && c.state == MqttConnectionState.connected);
    return rc;
  }

  group('malformed packets -> DISCONNECT 0x81 (section 4.13)', () {
    final cases = <String, List<int>>{
      'unknown packet type 0': [0x00, 0x00],
      'reserved fixed-header flags on PUBACK': [0x42, 0x02, 0x00, 0x01],
      'Remaining Length longer than 4 bytes': [
        0xD0,
        0xFF,
        0xFF,
        0xFF,
        0xFF,
        0x7F
      ],
      'Remaining Length not minimally encoded': [0xD0, 0x80, 0x00],
      'trailing bytes after PINGRESP': [0xD0, 0x01, 0x00],
      'PUBLISH with QoS 3': _publish('x', qos: 3),
      'QoS 0 PUBLISH with DUP set': _publish('x', flags: 0x08),
      'QoS 1 PUBLISH with packet identifier 0': _publish('x', qos: 1, pid: 0),
      'PUBACK with packet identifier 0': [0x40, 0x02, 0x00, 0x00],
      'malformed UTF-8 in a topic': rawPacket(0x30, [0, 2, 0xC3, 0x28, 0]),
      'U+0000 in a topic': rawPacket(0x30, [0, 2, 0x61, 0x00, 0]),
      'unknown property identifier': _publish('x', props: [0x7F, 0x00]),
    };
    for (final e in cases.entries) {
      test(e.key, () async {
        final c = await connected();
        expect(await injectExpectingDisconnect(c, e.value), 0x81);
        expect(c.metrics.protocolErrorCount, greaterThanOrEqualTo(1));
        await c.close();
      });
    }

    test('a property section longer than the packet is malformed (0x81)',
        () async {
      final c = await connected();
      expect(
          await injectExpectingDisconnect(
              c, rawPacket(0x30, [...rawString('x'), 0x30, 0x03, 0x00])),
          0x81);
      await c.close();
    });

    test('a body cut short inside its declared length is malformed (0x81)',
        () async {
      final c = await connected();
      // PUBACK with Remaining Length 1: only half a packet identifier.
      expect(await injectExpectingDisconnect(c, [0x40, 0x01, 0x00]), 0x81);
      await c.close();
    });

    test(
        'a property not permitted in the packet type is malformed (0x81, '
        'section 2.2.2.2)', () async {
      final c = await connected();
      // Session Expiry Interval inside a PUBLISH.
      expect(
          await injectExpectingDisconnect(
              c, _publish('x', props: [0x11, 0, 0, 0, 1])),
          0x81);
      await c.close();
    });
  });

  group('protocol errors -> DISCONNECT 0x82', () {
    final cases = <String, List<int>>{
      'second CONNACK': [0x20, 0x03, 0x00, 0x00, 0x00],
      'PINGREQ from the server': [0xC0, 0x00],
      'SUBSCRIBE from the server':
          rawPacket(0x82, [0, 1, 0, ...rawString('a'), 0]),
      'AUTH without an Authentication Method in CONNECT': [0xF0, 0x00],
      'PUBLISH with an empty topic and no alias': _publish(''),
      'duplicate Content Type in PUBLISH':
          _publish('x', props: [0x03, 0, 1, 0x61, 0x03, 0, 1, 0x62]),
      'PUBACK with a reason code not allowed in PUBACK': [
        0x40,
        0x03,
        0x00,
        0x01,
        0x05
      ],
      'DISCONNECT with an undefined reason code': [0xE0, 0x01, 0x05],
      'AUTH with an undefined reason code': [0xF0, 0x01, 0x05],
      'boolean property out of range (Payload Format Indicator 2)':
          _publish('x', props: [0x01, 0x02]),
    };
    for (final e in cases.entries) {
      test(e.key, () async {
        final c = await connected();
        expect(await injectExpectingDisconnect(c, e.value), 0x82);
        await c.close();
      });
    }

    test('wildcard in a PUBLISH Topic Name (0x82 or 0x90)', () async {
      final c = await connected();
      expect(await injectExpectingDisconnect(c, _publish('a/+')),
          anyOf(0x82, 0x90));
      await c.close();
    });

    test('PUBLISH with an unknown topic alias inside the maximum', () async {
      // Alias 3 is inside Topic Alias Maximum 5, but the client has no
      // mapping. That is a protocol error (0x82), not 0x94.
      final c = await connected(topicAliasMaximum: 5);
      expect(
          await injectExpectingDisconnect(
              c, _publish('', props: [0x23, 0x00, 0x03])),
          0x82);
      await c.close();
    });
  });

  group('Topic Alias errors -> DISCONNECT 0x94 (section 3.3.2.3.4)', () {
    test('alias above the client Topic Alias Maximum', () async {
      final c = await connected(topicAliasMaximum: 5);
      expect(
          await injectExpectingDisconnect(
              c, _publish('a', props: [0x23, 0x00, 0x09])),
          0x94);
      await c.close();
    });

    test('alias 0', () async {
      final c = await connected(topicAliasMaximum: 5);
      expect(
          await injectExpectingDisconnect(
              c, _publish('a', props: [0x23, 0x00, 0x00])),
          0x94);
      await c.close();
    });

    test('a broker alias is honoured when it is within the maximum', () async {
      final c = await connected(topicAliasMaximum: 5);
      final inbox = Inbox(c);
      proxy.injectToClient(_publish('alias/t', props: [0x23, 0x00, 0x02]));
      proxy.injectToClient(_publish('', props: [0x23, 0x00, 0x02]));
      // Re-binding the same alias to another topic is allowed.
      proxy.injectToClient(_publish('alias/u', props: [0x23, 0x00, 0x02]));
      proxy.injectToClient(_publish('', props: [0x23, 0x00, 0x02]));
      await inbox.waitFor(4);
      expect(inbox.messages.map((m) => m.topic),
          ['alias/t', 'alias/t', 'alias/u', 'alias/u']);
      expect(c.state, MqttConnectionState.connected);
      await c.close();
    });
  });

  group('limits the client declared', () {
    test('packet above the client Maximum Packet Size -> 0x95', () async {
      final c = await connected(maximumPacketSize: 100);
      expect(
          await injectExpectingDisconnect(
              c, _publish('x', payload: List.filled(200, 0x61))),
          0x95);
      await c.close();
    });

    test('QoS 2 publications beyond the client Receive Maximum -> 0x93',
        () async {
      final c = await connected(receiveMaximum: 2);
      // Keep the client's PUBRECs for the fabricated publications away from
      // the broker, which has never heard of them.
      proxy.dropWhen =
          (f) => f.dir == Dir.c2s && f.type == kPubrec && f.packetId >= 900;
      proxy.injectToClient(_publish('rm', qos: 2, pid: 901));
      proxy.injectToClient(_publish('rm', qos: 2, pid: 902));
      // A retransmission of an exchange already in progress is not new.
      proxy.injectToClient(_publish('rm', qos: 2, pid: 902, flags: 0x08));
      await settle(200);
      expect(c.state, MqttConnectionState.connected);
      expect(
          await injectExpectingDisconnect(c, _publish('rm', qos: 2, pid: 903)),
          0x93);
      await c.close();
    });

    test('a QoS 1 PUBLISH counts against the same Receive Maximum', () async {
      final c = await connected(receiveMaximum: 1);
      proxy.dropWhen =
          (f) => f.dir == Dir.c2s && f.type == kPubrec && f.packetId >= 900;
      proxy.injectToClient(_publish('rm', qos: 2, pid: 911));
      expect(
          await injectExpectingDisconnect(c, _publish('rm', qos: 1, pid: 912)),
          0x93);
      await c.close();
    });
  });

  group('acknowledgements the client is not waiting for', () {
    test(
        'unknown PUBACK/PUBREC/PUBCOMP/SUBACK/UNSUBACK are ignored; unknown '
        'PUBREL is answered with PUBCOMP 0x92', () async {
      final c = await connected();
      proxy.dropWhen = (f) => f.dir == Dir.c2s && f.type == kPubcomp;
      proxy.injectToClient([0x40, 0x02, 0x03, 0xE7]); // PUBACK 999
      proxy.injectToClient([0x50, 0x02, 0x03, 0xE6]); // PUBREC 998
      proxy.injectToClient([0x70, 0x02, 0x03, 0xE5]); // PUBCOMP 997
      proxy.injectToClient([0x90, 0x04, 0x03, 0xE4, 0x00, 0x00]); // SUBACK 996
      proxy
          .injectToClient([0xB0, 0x04, 0x03, 0xE3, 0x00, 0x00]); // UNSUBACK 995
      proxy.injectToClient([0x62, 0x02, 0x03, 0xE2]); // PUBREL 994
      final pubcomp = await proxy.next(
          (f) => f.dir == Dir.c2s && f.type == kPubcomp,
          includeHistory: true);
      expect(pubcomp.packetId, 994);
      expect(pubcomp.ackReasonCode, 0x92);
      await settle(200);
      expect(proxy.sent(kDisconnect), isEmpty);
      expect(c.state, MqttConnectionState.connected);
      // The client still works.
      final r = await c.publish('ack/t', bytes('x'), qos: MqttQos.atLeastOnce);
      expect(r.reasonCode, isNotNull);
      await c.close();
    });

    test(
        'SUBACK / UNSUBACK with the wrong number of reason codes fail the '
        'call and disconnect', () async {
      // A count mismatch is a protocol error, so the connection ends. The
      // two packets have to be checked on separate connections.
      List<Uint8List>? mismatch(int type, WireFrame f) => f.type == type
          ? [
              Uint8List.fromList(rawPacket(
                  f.bytes[0], [f.bytes[2], f.bytes[3], 0x00, 0x00, 0x00]))
            ]
          : null;

      Future<void> expectProtocolDisconnect(Future<void> call) async {
        final before = proxy.sent(kDisconnect).length;
        await expectLater(call, throwsA(isA<MqttProtocolException>()));
        await waitUntil(() => proxy.sent(kDisconnect).length == before + 1);
        expect(proxy.sent(kDisconnect).last.ackReasonCode, 0x82);
      }

      final sub = await connected(autoReconnect: false);
      proxy.rewrite = (f) => mismatch(kSuback, f);
      await expectProtocolDisconnect(sub.subscribe('cnt/a'));
      expect(sub.state, MqttConnectionState.disconnected);
      await sub.close();

      final unsub = await connected(autoReconnect: false);
      proxy.rewrite = (f) => mismatch(kUnsuback, f);
      await unsub.subscribe('cnt/b');
      await expectProtocolDisconnect(unsub.unsubscribe(['cnt/b']));
      expect(unsub.state, MqttConnectionState.disconnected);
      await unsub.close();
    });

    test('SUBACK / UNSUBACK carrying codes not valid for them -> 0x82',
        () async {
      for (final type in [kSuback, kUnsuback]) {
        final c = await connected();
        proxy.rewrite = (f) => f.type == type
            ? [
                Uint8List.fromList(
                    rawPacket(f.bytes[0], [f.bytes[2], f.bytes[3], 0x00, 0x05]))
              ]
            : null;
        final op = type == kSuback
            ? c.subscribe('bad/rc')
            : c.subscribe('bad/rc').then((_) => c.unsubscribe(['bad/rc']));
        op.ignore();
        expect(await clientDisconnectReason(), 0x82);
        proxy.rewrite = null;
        await c.close();
        proxy.frames.clear();
      }
    });

    test('a PUBACK with a reason code not valid for it ends the connection',
        () async {
      final c = await connected();
      proxy.rewrite = (f) => f.type == kPuback
          ? [
              Uint8List.fromList([0x40, 0x03, f.bytes[2], f.bytes[3], 0x05])
            ]
          : null;
      c.publish('rc/t', bytes('x'), qos: MqttQos.atLeastOnce).ignore();
      expect(await clientDisconnectReason(), 0x82);
      await c.close();
    });
  });

  group('CONNACK tampering (handshake)', () {
    Future<Object?> connectWithConnack(List<int> Function(Uint8List) mutate,
        {bool cleanStart = true}) async {
      var once = false;
      proxy.rewrite = (f) {
        if (f.type != kConnack || once) return null;
        once = true;
        return [Uint8List.fromList(mutate(f.bytes))];
      };
      final c = newClient(proxy.port, clientId: 'ct${seq++}');
      Object? error;
      try {
        await c.connect(cleanStart: cleanStart);
      } on Object catch (e) {
        error = e;
      }
      await settle(300);
      await c.close();
      return error;
    }

    test(
        'Session Present on a Clean Start connection is refused and fatal '
        '(MQTT-3.2.2-4)', () async {
      final error =
          await connectWithConnack((b) => [b[0], b[1], 0x01, ...b.sublist(3)]);
      expect(error, isA<MqttProtocolException>());
      expect(await clientDisconnectReason(), 0x82);
      expect(proxy.sent(kConnect), hasLength(1));
    });

    final fatal = <String, List<int>>{
      'reserved CONNACK flags': [0x20, 0x03, 0x02, 0x00, 0x00],
      'reason code undefined in MQTT': [0x20, 0x03, 0x00, 0x05, 0x00],
      'reason code defined but not valid in CONNACK (0x01)': [
        0x20,
        0x03,
        0x00,
        0x01,
        0x00
      ],
      'failure code together with Session Present': [
        0x20,
        0x03,
        0x01,
        0x80,
        0x00
      ],
      'Maximum QoS property 2': rawPacket(0x20, [
        0x00,
        0x00,
        ...rawProps([0x24, 0x02])
      ]),
      'Receive Maximum property 0': rawPacket(0x20, [
        0x00,
        0x00,
        ...rawProps([0x21, 0, 0])
      ]),
    };
    for (final e in fatal.entries) {
      test(e.key, () async {
        final error = await connectWithConnack((_) => e.value);
        expect(error, isA<MqttProtocolException>());
        expect(proxy.sent(kConnect), hasLength(1),
            reason: 'a protocol error must not be retried');
      });
    }

    test('a truncated CONNACK is malformed and not retried', () async {
      final error = await connectWithConnack((_) => [0x20, 0x01, 0x00]);
      expect(error, isA<MqttProtocolException>());
      expect(proxy.sent(kConnect), hasLength(1));
    });

    test(
        'CONNACK 0x9C Use another server: MqttServerMovedException with the '
        'Server Reference, not retried', () async {
      final error = await connectWithConnack((_) => rawPacket(0x20, [
            0x00,
            0x9C,
            ...rawProps([0x1C, ...rawString('other:1883')]),
          ]));
      expect(
          error,
          isA<MqttServerMovedException>()
              .having((e) => e.serverReference, 'ref', 'other:1883')
              .having((e) => e.reasonCode, 'rc', 0x9C));
      expect(proxy.sent(kConnect), hasLength(1));
    });

    test(
        'a retryable CONNACK rejection (0x89 Server busy) is retried and '
        'then succeeds', () async {
      var count = 0;
      proxy.rewrite = (f) => f.type == kConnack && count++ == 0
          ? [
              Uint8List.fromList([0x20, 0x03, 0x00, 0x89, 0x00])
            ]
          : null;
      final c = newClient(proxy.port, clientId: 'ct-busy');
      await c.connect();
      expect(proxy.sent(kConnect), hasLength(2));
      await c.close();
    });

    test('DISCONNECT instead of CONNACK is a rejection (retried for 0x89)',
        () async {
      var count = 0;
      proxy.rewrite = (f) => f.type == kConnack && count++ == 0
          ? [
              Uint8List.fromList([0xE0, 0x01, 0x89])
            ]
          : null;
      final c = newClient(proxy.port, clientId: 'ct-disc');
      await c.connect();
      expect(proxy.sent(kConnect), hasLength(2));
      await c.close();
    });

    test('a PUBLISH before CONNACK is a protocol error', () async {
      final error =
          await connectWithConnack((b) => [..._publish('early'), ...b]);
      expect(error, isA<MqttProtocolException>());
    });
  });

  group('server-initiated DISCONNECT', () {
    test(
        '0x9D Server moved: onServerMoved is called, the client stops, the '
        'error is reported with the Server Reference', () async {
      final c = await connected();
      final moved = Completer<(String?, MqttReasonCode)>();
      c.onServerMoved = (ref, rc) => moved.complete((ref, rc));
      final errors = <MqttErrorEvent>[];
      c.errorEvents.listen(errors.add);
      proxy.injectToClient(rawPacket(0xE0, [
        0x9D,
        ...rawProps([0x1C, ...rawString('backup:1883')]),
      ]));
      final (ref, rc) = await moved.future.timeout(const Duration(seconds: 5));
      expect(ref, 'backup:1883');
      expect(rc, MqttReasonCode.serverMoved);
      await waitUntil(() => c.state == MqttConnectionState.disconnected);
      await settle(500);
      expect(proxy.connections, 1);
      await waitUntil(() => errors.isNotEmpty);
      expect(errors.single.error, isA<MqttServerMovedException>());
      await c.close();
    });

    test(
        '0x87 Not authorized is fatal; Reason String and User Properties '
        'are logged safely', () async {
      final log = CollectingLogger();
      final c = newClient(proxy.port, clientId: 'sd-87', logger: log);
      await c.connect();
      final reason = 'bad\nline${'x' * 300}';
      proxy.injectToClient(rawPacket(0xE0, [
        0x87,
        ...rawProps([
          0x1F,
          ...rawString(reason),
          0x26,
          ...rawString('k'),
          ...rawString('v\t1'),
          0x1C,
          ...rawString('srv'),
        ]),
      ]));
      await waitUntil(() => c.state == MqttConnectionState.disconnected);
      await settle(300);
      expect(proxy.connections, 1);
      final line =
          log.lines.firstWhere((l) => l.contains('Broker sent DISCONNECT'));
      expect(line, contains('notAuthorized code=0x87'));
      expect(line, contains('userProperties={k=v 1}'));
      expect(line, isNot(contains('\n')));
      expect(line, contains('…'));
      await c.close();
    });

    test(
        'a DISCONNECT with no reason code means normal disconnection and '
        'the client reconnects', () async {
      final c = await connected();
      proxy.injectToClient([0xE0, 0x00]);
      await waitUntil(() =>
          proxy.connections >= 2 && c.state == MqttConnectionState.connected);
      await c.close();
    });
  });

  group('enhanced authentication tampering (real plugin)', () {
    late Mosquitto authBroker;
    late FaultProxy authProxy;
    setUp(() async {
      final so = await buildAuthPlugin();
      authBroker = await Mosquitto.start(config: ['plugin $so']);
      authProxy = await FaultProxy.start(authBroker.port);
    });
    tearDown(() async {
      await authProxy.close();
      await authBroker.dispose();
    });

    Uint8List authWith({required int rc, required String method}) {
      final props = [0x15, ...rawString(method), 0x16, ...rawString('x')];
      return Uint8List.fromList(rawPacket(0xF0, [rc, props.length, ...props]));
    }

    for (final (label, rc, method) in [
      ('a different Authentication Method', 0x18, 'OTHER'),
      ('reason code 0x19, which only a client may send', 0x19, 'TEST-CR'),
    ]) {
      test('handshake AUTH with $label is refused', () async {
        authProxy.rewrite = (f) => f.type == kAuth && f.dir == Dir.s2c
            ? [authWith(rc: rc, method: method)]
            : null;
        final c = newClient(authProxy.port,
            clientId: 'at${seq++}', authenticator: ProofAuthenticator());
        await expectLater(
            c.connect(
                authenticationMethod: 'TEST-CR',
                authenticationData: bytes('client-first')),
            throwsA(isA<MqttProtocolException>()));
        expect(authProxy.sent(kConnect), hasLength(1));
        await c.close();
      });

      test('re-authentication AUTH with $label ends the connection', () async {
        final c = newClient(authProxy.port,
            clientId: 'ar${seq++}', authenticator: ProofAuthenticator());
        await c.connect(
            authenticationMethod: 'TEST-CR',
            authenticationData: bytes('client-first'));
        authProxy.rewrite = (f) => f.type == kAuth && f.dir == Dir.s2c
            ? [authWith(rc: rc, method: method)]
            : null;
        c
            .reauthenticate(authenticationData: bytes('client-first-reauth'))
            .ignore();
        final d = await authProxy.next(
            (f) => f.dir == Dir.c2s && f.type == kDisconnect,
            includeHistory: true);
        expect(d.ackReasonCode, 0x82);
        await c.close();
      });
    }

    test('re-authentication timing out closes the connection', () async {
      final c = newClient(authProxy.port,
          clientId: 'ato',
          authenticator: ProofAuthenticator(),
          operationTimeout: const Duration(milliseconds: 500));
      await c.connect(
          authenticationMethod: 'TEST-CR',
          authenticationData: bytes('client-first'));
      authProxy.dropWhen = (f) => f.dir == Dir.s2c && f.type == kAuth;
      await expectLater(
          c.reauthenticate(authenticationData: bytes('client-first-reauth')),
          throwsA(isA<MqttTimeoutException>()));
      await waitUntil(() => c.state == MqttConnectionState.disconnected);
      await c.close();
    });

    test('re-authentication the authenticator abandons closes the connection',
        () async {
      final auth = _OnceAuthenticator();
      final c = newClient(authProxy.port, clientId: 'aab', authenticator: auth);
      await c.connect(
          authenticationMethod: 'TEST-CR',
          authenticationData: bytes('client-first'));
      await expectLater(
          c.reauthenticate(authenticationData: bytes('client-first-reauth')),
          throwsA(isA<MqttAuthenticationException>()));
      await waitUntil(() => c.state == MqttConnectionState.disconnected);
      await c.close();
    });

    test('only one re-authentication at a time; none while disconnected',
        () async {
      final c = newClient(authProxy.port,
          clientId: 'a2x', authenticator: ProofAuthenticator());
      await c.connect(
          authenticationMethod: 'TEST-CR',
          authenticationData: bytes('client-first'));
      final first =
          c.reauthenticate(authenticationData: bytes('client-first-reauth'));
      // The state check runs first, so a concurrent call is refused as "Not
      // connected" (the "already in progress" branch is unreachable).
      expect(() => c.reauthenticate(), throwsA(isA<MqttException>()));
      await first;
      await c.disconnect();
      expect(() => c.reauthenticate(), throwsA(isA<MqttConnectionException>()));
      await c.close();
    });
  });
}

/// Answers the handshake challenge, then gives up on the next one.
final class _OnceAuthenticator implements MqttAuthenticator {
  int calls = 0;
  @override
  Future<MqttAuthResponse?> authenticate(MqttAuthChallenge challenge) async =>
      calls++ == 0 ? MqttAuthResponse(bytes('proof:server-challenge')) : null;
}
