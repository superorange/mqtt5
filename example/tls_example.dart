// Demonstrates connecting over TLS with `SecureSocket`.
//
//   dart run example/tls_example.dart --host broker.example.com --port 8883 \
//     --ca ca.pem [--cert client.pem --key client.key]
//
// - --ca   PEM bundle of trusted CA certificates used to verify the server.
// - --cert PEM client certificate chain (mutual TLS).
// - --key  PEM client private key (mutual TLS).
import 'dart:convert';
import 'dart:io';

import 'package:mqtt5/mqtt5.dart';

Future<void> main(List<String> args) async {
  final options = _parseArgs(args);

  // Build the security context from PEM files.
  SecurityContext? securityContext;
  if (options.ca != null) {
    securityContext = TlsTransport.createSecurityContext(
      trustedCertificates: File(options.ca!).readAsStringSync(),
      certificateChain: options.cert == null
          ? null
          : File(options.cert!).readAsStringSync(),
      privateKey: options.key == null
          ? null
          : File(options.key!).readAsStringSync(),
    );
  }

  final client = MqttClient(
    host: options.host,
    port: options.port,
    clientId: options.clientId,
    useTls: true,
    securityContext: securityContext,
    // For development against a self-signed certificate only; never accept
    // every certificate in production.
    onBadCertificate: options.insecure
        ? (certificate) => true
        : null,
    // Application-Layer Protocol Negotiation (e.g. 'mqtt' or 'http/1.1').
    alpnProtocols: const ['mqtt'],
  );

  client.messages.listen((message) {
    print('[message] ${message.topic}: ${utf8.decode(message.payload)}');
  });

  await client.connect(keepAlive: const Duration(seconds: 30));
  print('TLS connection established');

  await client.subscribe('tls/topic');
  await client.publish('tls/topic', utf8.encode('hello over TLS'));

  await Future<void>.delayed(const Duration(seconds: 1));
  await client.disconnect();
}

({String host, int port, String clientId, String? ca, String? cert, String? key, bool insecure}) _parseArgs(
  List<String> args,
) {
  var host = '127.0.0.1';
  var port = 8883;
  var clientId = 'tls-example';
  String? ca;
  String? cert;
  String? key;
  var insecure = false;

  for (var i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--host':
        host = args[++i];
      case '--port':
        port = int.parse(args[++i]);
      case '--client-id':
        clientId = args[++i];
      case '--ca':
        ca = args[++i];
      case '--cert':
        cert = args[++i];
      case '--key':
        key = args[++i];
      case '--insecure':
        insecure = true;
    }
  }

  return (
    host: host,
    port: port,
    clientId: clientId,
    ca: ca,
    cert: cert,
    key: key,
    insecure: insecure,
  );
}
