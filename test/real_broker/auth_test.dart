@Tags(['real-broker'])
@Timeout(Duration(seconds: 60))
library;

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'support/common.dart';

/// Answers the TEST-CR challenge implemented by support/plugin/ext_auth.c.
final class ProofAuthenticator implements MqttAuthenticator {
  ProofAuthenticator({this.proof});
  final String? proof;
  final List<MqttAuthChallenge> challenges = [];

  @override
  Future<MqttAuthResponse?> authenticate(MqttAuthChallenge challenge) async {
    challenges.add(challenge);
    await Future<void>.delayed(const Duration(milliseconds: 20));
    final p = proof ?? 'proof:${utf8.decode(challenge.data ?? const [])}';
    return MqttAuthResponse(Uint8List.fromList(utf8.encode(p)));
  }
}

final class RefusingAuthenticator implements MqttAuthenticator {
  @override
  Future<MqttAuthResponse?> authenticate(MqttAuthChallenge challenge) async =>
      null;
}

void main() {
  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
    return;
  }

  late String pluginPath;
  setUpAll(() async => pluginPath = await buildAuthPlugin());

  late Mosquitto broker;
  late FaultProxy proxy;
  setUp(() async {
    broker = await Mosquitto.start(config: ['plugin $pluginPath']);
    proxy = await FaultProxy.start(broker.port);
  });
  tearDown(() async {
    await proxy.close();
    await broker.dispose();
  });

  test('single-step method: CONNACK carries the server Authentication Data',
      () async {
    final c = newClient(proxy.port, clientId: 'a1');
    await c.connect(
        authenticationMethod: 'TEST-ONE', authenticationData: bytes('ok'));
    expect(c.state, MqttConnectionState.connected);
    final data = c.connackProperties.whereType<AuthenticationData>().single;
    expect(text(data.data), 'welcome');
    final method = c.connackProperties.whereType<AuthenticationMethod>().single;
    expect(method.value, 'TEST-ONE');
    expect(proxy.sent(kAuth), isEmpty);
    await c.close();
  });

  test('single-step method rejected: not retried', () async {
    final c = newClient(proxy.port, clientId: 'a2');
    await expectLater(
      c.connect(
          authenticationMethod: 'TEST-ONE', authenticationData: bytes('no')),
      throwsA(isA<MqttServerRejectedException>()
          .having((e) => e.reasonCode, 'rc', anyOf(0x86, 0x87))),
    );
    await settle(500);
    expect(proxy.sent(kConnect), hasLength(1));
    await c.close();
  });

  test(
      'challenge/response: AUTH 0x18 exchange then CONNACK, with the '
      'method echoed on every AUTH', () async {
    final auth = ProofAuthenticator();
    final c = newClient(proxy.port, clientId: 'a3', authenticator: auth);
    final states = <MqttConnectionState>[];
    c.stateStream.listen(states.add);
    await c.connect(
        authenticationMethod: 'TEST-CR',
        authenticationData: bytes('client-first'));
    expect(c.state, MqttConnectionState.connected);
    expect(states, contains(MqttConnectionState.authenticating));
    expect(auth.challenges.single.method, 'TEST-CR');
    expect(text(auth.challenges.single.data!), 'server-challenge');

    final serverAuth = proxy.received(kAuth).single;
    expect(serverAuth.ackReasonCode, 0x18);
    final clientAuth = proxy.sent(kAuth).single;
    expect(clientAuth.ackReasonCode, 0x18);
    expect(prop(clientAuth.ackProperties, 0x15), 'TEST-CR');
    expect(utf8.decode(prop(clientAuth.ackProperties, 0x16) as List<int>),
        'proof:server-challenge');
    final final_ = c.connackProperties.whereType<AuthenticationData>().single;
    expect(text(final_.data), 'server-final');

    // The authenticated connection is usable.
    final inbox = Inbox(c);
    await c.subscribe('a3/t');
    await c.publish('a3/t', bytes('x'), qos: MqttQos.atLeastOnce);
    await inbox.waitFor(1);
    await c.close();
  });

  test('wrong proof: CONNACK failure, fatal', () async {
    final c = newClient(proxy.port,
        clientId: 'a4', authenticator: ProofAuthenticator(proof: 'wrong'));
    await expectLater(
      c.connect(
          authenticationMethod: 'TEST-CR',
          authenticationData: bytes('client-first')),
      throwsA(isA<MqttServerRejectedException>()),
    );
    await settle(500);
    expect(proxy.sent(kConnect), hasLength(1));
    expect(c.state, MqttConnectionState.disconnected);
    await c.close();
  });

  test(
      'authenticator aborting the exchange closes the connection and '
      'surfaces MqttAuthenticationException', () async {
    final c = newClient(proxy.port,
        clientId: 'a5', authenticator: RefusingAuthenticator());
    await expectLater(
      c.connect(
          authenticationMethod: 'TEST-CR',
          authenticationData: bytes('client-first')),
      throwsA(isA<MqttAuthenticationException>()),
    );
    await settle(300);
    expect(proxy.sent(kConnect), hasLength(1));
    expect(proxy.sent(kAuth), isEmpty);
    printOnFailure(proxy.dump());
    await c.close();
  });

  test('a challenge with no authenticator configured fails cleanly', () async {
    final c = newClient(proxy.port, clientId: 'a6');
    await expectLater(
      c.connect(
          authenticationMethod: 'TEST-CR',
          authenticationData: bytes('client-first')),
      throwsA(isA<MqttAuthenticationException>()),
    );
    expect(c.state, MqttConnectionState.disconnected);
    await c.close();
  });

  // Section 4.12.1 lets other packets flow during re-authentication, but
  // mosquitto 2.1.2 answers a PUBLISH sent mid-exchange with DISCONNECT 0x82
  // ("PUBLISH before session is active"), so traffic is exchanged before and
  // after the exchange here rather than during it.
  test('re-authentication (AUTH 0x19) succeeds and the connection stays usable',
      () async {
    final auth = ProofAuthenticator();
    final c = newClient(proxy.port, clientId: 'a7', authenticator: auth);
    await c.connect(
        authenticationMethod: 'TEST-CR',
        authenticationData: bytes('client-first'));
    final inbox = Inbox(c);
    await c.subscribe('a7/t');
    final states = <MqttConnectionState>[];
    c.stateStream.listen(states.add);
    await c.reauthenticate(authenticationData: bytes('client-first-reauth'));
    expect(states, [
      MqttConnectionState.authenticating,
      MqttConnectionState.connected,
    ]);
    await c.publish('a7/t', bytes('after'), qos: MqttQos.atLeastOnce);
    await inbox.waitFor(1);
    expect(c.state, MqttConnectionState.connected);
    expect(proxy.sent(kAuth).map((f) => f.ackReasonCode).toList(),
        [0x18, 0x19, 0x18]);
    final start = proxy.sent(kAuth).elementAt(1);
    expect(prop(start.ackProperties, 0x15), 'TEST-CR');
    expect(proxy.received(kAuth).map((f) => f.ackReasonCode).toList(),
        [0x18, 0x18, 0x00]);
    expect(auth.challenges, hasLength(2));
    await broker.waitForLog('Received AUTH from a7');
    await c.close();
  });

  test(
      're-authentication rejected by the broker throws and ends the '
      'connection', () async {
    final c = newClient(proxy.port,
        clientId: 'a8', authenticator: ProofAuthenticator());
    await c.connect(
        authenticationMethod: 'TEST-CR',
        authenticationData: bytes('client-first'));
    final errors = <Object>[];
    c.errors.listen(errors.add);
    await expectLater(c.reauthenticate(authenticationData: bytes('deny')),
        throwsA(isA<MqttException>()));
    await waitUntil(() => c.state == MqttConnectionState.disconnected);
    printOnFailure(proxy.dump());
    await c.close();
  });

  test(
      're-authentication without an Authentication Method is refused '
      'locally (MQTT-4.12.0-7)', () async {
    final c = newClient(proxy.port, clientId: 'a9');
    await c.connect();
    await expectLater(
        c.reauthenticate(), throwsA(isA<MqttAuthenticationException>()));
    expect(proxy.sent(kAuth), isEmpty);
    expect(c.state, MqttConnectionState.connected);
    await c.close();
  });

  test('reconnect repeats the full enhanced-auth handshake', () async {
    final auth = ProofAuthenticator();
    final c = newClient(proxy.port, clientId: 'a10', authenticator: auth);
    await c.connect(
        authenticationMethod: 'TEST-CR',
        authenticationData: bytes('client-first'));
    proxy.cutAll();
    await waitUntil(() =>
        proxy.connections >= 2 && c.state == MqttConnectionState.connected);
    expect(auth.challenges, hasLength(2));
    await c.close();
  });
}
