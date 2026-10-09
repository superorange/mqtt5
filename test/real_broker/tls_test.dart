@Tags(['real-broker'])
@Timeout(Duration(seconds: 60))
library;

import 'dart:io';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'support/common.dart';

Future<void> _openssl(List<String> args, String cwd) async {
  final r = await Process.run('openssl', args, workingDirectory: cwd);
  if (r.exitCode != 0) {
    throw StateError('openssl ${args.join(' ')} failed:\n${r.stderr}');
  }
}

/// Creates a CA, a server certificate for [san] and a client certificate.
Future<Directory> _makePki(String san) async {
  final d = await Directory.systemTemp.createTemp('mqtt5_pki_');
  final p = d.path;
  // macOS verifies through the Security framework, which insists on proper
  // key usage extensions.
  await _openssl([
    'req',
    '-x509',
    '-newkey',
    'rsa:2048',
    '-nodes',
    '-days',
    '2',
    '-keyout',
    'ca.key',
    '-out',
    'ca.crt',
    '-subj',
    '/CN=mqtt5-test-ca',
    '-addext',
    'basicConstraints=critical,CA:TRUE',
    '-addext',
    'keyUsage=critical,keyCertSign,cRLSign'
  ], p);
  await File('$p/server.cnf')
      .writeAsString('subjectAltName=$san\nextendedKeyUsage=serverAuth\n'
          'keyUsage=critical,digitalSignature,keyEncipherment\n'
          'basicConstraints=CA:FALSE\n');
  await File('$p/client.cnf').writeAsString('extendedKeyUsage=clientAuth\n'
      'keyUsage=critical,digitalSignature,keyEncipherment\n'
      'basicConstraints=CA:FALSE\n');
  for (final name in ['server', 'client']) {
    await _openssl([
      'req',
      '-newkey',
      'rsa:2048',
      '-nodes',
      '-keyout',
      '$name.key',
      '-out',
      '$name.csr',
      '-subj',
      '/CN=$name'
    ], p);
    await _openssl([
      'x509',
      '-req',
      '-in',
      '$name.csr',
      '-CA',
      'ca.crt',
      '-CAkey',
      'ca.key',
      '-CAcreateserial',
      '-out',
      '$name.crt',
      '-days',
      '2',
      '-extfile',
      '$name.cnf'
    ], p);
  }
  return d;
}

void main() {
  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
    return;
  }

  late Directory pki;
  late Directory wrongNamePki;
  setUpAll(() async {
    pki = await _makePki('IP:127.0.0.1,DNS:localhost');
    wrongNamePki = await _makePki('DNS:other.example');
  });
  tearDownAll(() async {
    await pki.delete(recursive: true);
    await wrongNamePki.delete(recursive: true);
  });

  late Mosquitto broker;
  tearDown(() => broker.dispose());

  Future<Mosquitto> tlsBroker(Directory d, {bool mtls = false}) =>
      Mosquitto.start(listenerConfig: [
        'cafile ${d.path}/ca.crt',
        'certfile ${d.path}/server.crt',
        'keyfile ${d.path}/server.key',
        if (mtls) 'require_certificate true',
      ]);

  String pem(Directory d, String name) =>
      File('${d.path}/$name').readAsStringSync();

  MqttClient tlsClient(int port,
          {SecurityContext? context,
          bool Function(X509Certificate)? onBad,
          bool autoReconnect = true,
          String clientId = 'tls'}) =>
      MqttClient(
        host: '127.0.0.1',
        port: port,
        clientId: clientId,
        useTls: true,
        securityContext: context,
        onBadCertificate: onBad,
        autoReconnect: autoReconnect,
        connectionTimeout: const Duration(seconds: 2),
        reconnectManager: fastReconnect(),
      );

  test('TLS with a private CA: connect, publish, receive', () async {
    broker = await tlsBroker(pki);
    final c = tlsClient(broker.port,
        context: TlsTransport.createSecurityContext(
            trustedCertificates: pem(pki, 'ca.crt')));
    await c.connect();
    final inbox = Inbox(c);
    await c.subscribe('tls/t',
        options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
    await c.publish('tls/t', bytes('secure'), qos: MqttQos.exactlyOnce);
    await inbox.waitFor(1);
    expect(inbox.payloads, ['secure']);
    await c.close();
  });

  test('untrusted server certificate fails once, without a retry loop',
      () async {
    broker = await tlsBroker(pki);
    final c = tlsClient(broker.port);
    await expectLater(c.connect(), throwsA(isA<TlsException>()));
    await settle(500);
    expect(broker.countLog('New connection from'), lessThanOrEqualTo(2));
    expect(c.state, MqttConnectionState.disconnected);
    await c.close();
  });

  test('onBadCertificate can accept an untrusted certificate', () async {
    broker = await tlsBroker(pki);
    X509Certificate? seen;
    final c = tlsClient(broker.port, onBad: (cert) {
      seen = cert;
      return true;
    });
    await c.connect();
    expect(seen, isNotNull);
    await c.close();
  });

  test('host name mismatch is rejected even with a trusted CA', () async {
    broker = await tlsBroker(wrongNamePki);
    final c = tlsClient(broker.port,
        autoReconnect: false,
        context: TlsTransport.createSecurityContext(
            trustedCertificates: pem(wrongNamePki, 'ca.crt')));
    Object? error;
    try {
      await c.connect();
    } on Object catch (e) {
      error = e;
    }
    // macOS reports every platform trust failure the same way; the only
    // difference from the passing first test is the certificate's SAN.
    expect(error, isA<TlsException>());
    expect('$error', contains('CERTIFICATE_VERIFY_FAILED'));
    await c.close();
  });

  test('mutual TLS: client certificate required and accepted', () async {
    broker = await tlsBroker(pki, mtls: true);
    final ok = tlsClient(broker.port,
        context: TlsTransport.createSecurityContext(
          trustedCertificates: pem(pki, 'ca.crt'),
          certificateChain: pem(pki, 'client.crt'),
          privateKey: pem(pki, 'client.key'),
        ));
    await ok.connect();
    expect(ok.state, MqttConnectionState.connected);
    await ok.close();

    final noCert = tlsClient(broker.port,
        clientId: 'tls-nocert',
        autoReconnect: false,
        context: TlsTransport.createSecurityContext(
            trustedCertificates: pem(pki, 'ca.crt')));
    Object? error;
    try {
      await noCert.connect();
    } on Object catch (e) {
      error = e;
    }
    expect(error, isNotNull);
    expect(noCert.state, MqttConnectionState.disconnected);
    await noCert.close();
  });

  test('a TLS peer that never answers the handshake times out', () async {
    broker = await Mosquitto.start();
    final proxy = await FaultProxy.start(broker.port)
      ..blackhole = true;
    final c = tlsClient(proxy.port, autoReconnect: false);
    final sw = Stopwatch()..start();
    await expectLater(c.connect(), throwsA(isA<SocketException>()));
    expect(sw.elapsedMilliseconds, lessThan(4000));
    await c.close();
    await proxy.close();
  });
}
