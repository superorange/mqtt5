// A chaos test that randomly restarts a local mosquitto broker while a client
// keeps publishing, to verify the client state machine recovers.
//
// Usage:
//   dart run tool/chaos_test.dart [--rounds 20] [--mosquitto /path/to/mosquitto]
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';

const String _mosquitto = '/opt/homebrew/sbin/mosquitto';

Future<void> main(List<String> args) async {
  var rounds = 20;
  var mosquitto = _mosquitto;
  for (var i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--rounds':
        rounds = int.parse(args[++i]);
      case '--mosquitto':
        mosquitto = args[++i];
    }
  }
  if (!File(mosquitto).existsSync()) {
    stderr.writeln('mosquitto not found at $mosquitto');
    exit(1);
  }

  final port = await _freePort();
  final dir = await Directory.systemTemp.createTemp('mqtt5-chaos');
  final config = File('${dir.path}/mosquitto.conf');
  await config.writeAsString('listener $port 127.0.0.1\nallow_anonymous true\n');

  var broker = await _startBroker(mosquitto, config.path, port);
  final random = Random();

  final client = MqttClient(
    host: '127.0.0.1',
    port: port,
    clientId: 'chaos-client',
    reconnectManager: ReconnectManager(
      initialDelay: const Duration(milliseconds: 200),
      maxDelay: const Duration(seconds: 2),
    ),
  );
  await client.connect(cleanStart: false, keepAlive: const Duration(seconds: 2));
  await client.subscribe('chaos/topic', options: const MqttSubscriptionOptions(
    qos: MqttQos.atLeastOnce,
  ));

  var published = 0;
  var received = 0;
  client.messages.listen((m) => received++);

  final publisher = Timer.periodic(const Duration(milliseconds: 100), (t) {
    client
        .publish(
          'chaos/topic',
          Uint8List.fromList(utf8.encode('msg-$published')),
          qos: MqttQos.atLeastOnce,
        )
        .then<void>((_) {}, onError: (Object e) {
      // Connection lost mid-publish is expected during chaos.
    });
    published++;
  });

  for (var round = 0; round < rounds; round++) {
    await Future<void>.delayed(
      Duration(milliseconds: 300 + random.nextInt(1200)),
    );
    // Restart the broker.
    stdout.writeln('round $round: restarting broker');
    broker.kill();
    broker = await _startBroker(mosquitto, config.path, port);
    await _waitFor(() => client.state == MqttConnectionState.connected,
        const Duration(seconds: 10));
    stdout.writeln(
      'round $round: reconnected (state=${client.state.name}, '
      'reconnects=${client.metrics.reconnectCount})',
    );
  }

  publisher.cancel();
  await Future<void>.delayed(const Duration(seconds: 1));
  stdout.writeln(
    'Chaos test complete: published=$published received=$received '
    'reconnects=${client.metrics.reconnectCount}',
  );
  await client.disconnect();
  broker.kill();
  exit(0);
}

Future<Process> _startBroker(String mosquitto, String config, int port) async {
  final process = await Process.start(mosquitto, ['-c', config]);
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (true) {
    try {
      final socket = await Socket.connect(
        '127.0.0.1',
        port,
        timeout: const Duration(milliseconds: 200),
      );
      socket.destroy();
      return process;
    } on SocketException {
      if (DateTime.now().isAfter(deadline)) {
        process.kill();
        throw StateError('broker did not start');
      }
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
  }
}

Future<int> _freePort() async {
  final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final port = server.port;
  await server.close();
  return port;
}

Future<void> _waitFor(bool Function() condition, Duration timeout) async {
  final deadline = DateTime.now().add(timeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      throw StateError('timed out waiting for condition');
    }
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }
}
