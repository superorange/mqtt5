// Demonstrates MQTT 5.0 enhanced authentication (AUTH packet exchange).
//
// The broker challenges the client; the client responds with the next round
// of authentication data until the broker accepts (CONNACK success) or
// rejects the connection.
//
//   dart run example/enhanced_auth_example.dart --host broker.example.com
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';

Future<void> main(List<String> args) async {
  final options = _parseArgs(args);

  final client = MqttClient(
    host: options.host,
    port: options.port,
    clientId: options.clientId,
    // Provide the challenge/response callback. The library drives the
    // AUTH exchange automatically during connect() and handles re-auth.
    authenticator: DemoAuthenticator(),
  );

  client.messages.listen((message) {
    print('[message] ${message.topic}: ${utf8.decode(message.payload)}');
  });

  try {
    await client.connect(
      keepAlive: const Duration(seconds: 30),
      authenticationMethod: 'DEMO-SASL',
      authenticationData: Uint8List.fromList(utf8.encode('initial-data')),
    );
    print('authenticated and connected');

    await client.subscribe('auth/topic');
    await client.publish('auth/topic', utf8.encode('hello'));
    await Future<void>.delayed(const Duration(seconds: 1));
  } on MqttAuthenticationException catch (e) {
    print('authentication failed: $e');
  } finally {
    await client.disconnect();
  }
}

/// A trivial authenticator that just echoes the challenge back with a fixed
/// response. Replace with a real SASL/SCRAM/etc. implementation.
final class DemoAuthenticator implements MqttAuthenticator {
  int _round = 0;

  @override
  Future<MqttAuthResponse?> authenticate(MqttAuthChallenge challenge) async {
    _round++;
    print('challenge #$_round: method=${challenge.method} '
        'data=${challenge.data == null ? null : utf8.decode(challenge.data!)}');

    // Return null to abort authentication with an error.
    if (_round > 3) {
      return null;
    }

    return MqttAuthResponse(
      Uint8List.fromList(utf8.encode('client-response-$_round')),
    );
  }
}

({String host, int port, String clientId}) _parseArgs(List<String> args) {
  var host = '127.0.0.1';
  var port = 1883;
  var clientId = 'auth-example';

  for (var i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--host':
        host = args[++i];
      case '--port':
        port = int.parse(args[++i]);
      case '--client-id':
        clientId = args[++i];
    }
  }

  return (host: host, port: port, clientId: clientId);
}
