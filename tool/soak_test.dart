// A long-running soak test.
//
// Usage:
//   dart run tool/soak_test.dart --host 127.0.0.1 --port 18883 \
//       --client-id soak-1 [--qos 1] [--rate 10] [--duration 3600]
//
// Continuously publishes and subscribes, tracking reconnect count, RTT and
// message rates. Intended for 24h/72h/7d runs.
import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';

Future<void> main(List<String> args) async {
  final options = _parseArgs(args);
  final random = Random();

  final client = MqttClient(
    host: options.host,
    port: options.port,
    clientId: options.clientId,
    reconnectManager: ReconnectManager(
      initialDelay: const Duration(seconds: 1),
      maxDelay: const Duration(seconds: 30),
    ),
  );

  var received = 0;
  var published = 0;
  var duplicates = 0;
  var acks = 0;
  var ackTimeouts = 0;
  client.messages.listen((message) {
    received++;
    if (message.duplicate) {
      duplicates++;
    }
  });

  final deadline = options.duration == 0
      ? null
      : DateTime.now().add(Duration(seconds: options.duration));

  await client.connect(
    cleanStart: false,
    keepAlive: const Duration(seconds: 10),
    sessionExpiryInterval: const Duration(hours: 1),
  );
  await client.subscribe('soak/topic', options: const MqttSubscriptionOptions(
    qos: MqttQos.atLeastOnce,
  ));

  final timer = Timer.periodic(const Duration(seconds: 1), (t) async {
    if (deadline != null && DateTime.now().isAfter(deadline)) {
      t.cancel();
      return;
    }
    try {
      await client
          .publish(
            'soak/topic',
            Uint8List.fromList(
              List.generate(64, (_) => random.nextInt(256)),
            ),
            qos: options.qos,
          )
          .timeout(const Duration(seconds: 5));
      published++;
      acks++;
    } on TimeoutException {
      ackTimeouts++;
    } on MqttException {
      // Publish failed (e.g. connection lost); the reconnect manager handles it.
    }
  });

  // Periodic status report.
  Timer.periodic(const Duration(seconds: 10), (t) {
    final m = client.metrics;
    stdout.writeln(
      '[${DateTime.now().toIso8601String()}] '
      'state=${client.state.name} '
      'published=$published received=$received duplicates=$duplicates '
      'acks=$acks ackTimeouts=$ackTimeouts inflight=${client.inflightCount} '
      'reconnects=${m.reconnectCount} pingRtt=${m.lastPingRtt} '
      'bytes(s/r)=${m.bytesSentB}/${m.bytesReceivedB}',
    );
    if (deadline != null && DateTime.now().isAfter(deadline)) {
      t.cancel();
      timer.cancel();
      stdout.writeln('Soak test finished');
      exit(0);
    }
  });

  while (deadline == null || DateTime.now().isBefore(deadline)) {
    await Future<void>.delayed(const Duration(seconds: 1));
  }
  stdout.writeln('Soak test finished');
  exit(0);
}

({String host, int port, String clientId, MqttQos qos, int duration}) _parseArgs(
  List<String> args,
) {
  var host = '127.0.0.1';
  var port = 1883;
  var clientId = 'soak-${Random().nextInt(1 << 32)}';
  var qos = MqttQos.atLeastOnce;
  var duration = 0;

  for (var i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--host':
        host = args[++i];
      case '--port':
        port = int.parse(args[++i]);
      case '--client-id':
        clientId = args[++i];
      case '--qos':
        qos = MqttQos.fromValue(int.parse(args[++i]));
      case '--duration':
        duration = int.parse(args[++i]);
    }
  }
  return (
    host: host,
    port: port,
    clientId: clientId,
    qos: qos,
    duration: duration,
  );
}
