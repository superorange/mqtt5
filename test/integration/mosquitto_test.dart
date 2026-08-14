import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:mqtt5/src/client/mqtt_client.dart';
import 'package:mqtt5/src/client/reconnect_manager.dart';
import 'package:mqtt5/src/mqtt_qos.dart';
import 'package:mqtt5/src/subscription.dart';
import 'package:test/test.dart';

const String _mosquitto = '/opt/homebrew/sbin/mosquitto';

void main() {
  if (!File(_mosquitto).existsSync()) {
    // Still declare the group so test output reflects the skip.
    group('mosquitto integration', () {
      test('skipped: mosquitto not installed', () {
        markTestSkipped('mosquitto binary not found at $_mosquitto');
      });
    });
    return;
  }

  group('mosquitto integration', () {
    test('connect, subscribe, publish, receive, disconnect', () async {
      final port = await _freePort();
      final broker = await _startBroker(port);

      try {
        final client = MqttClient(
          host: '127.0.0.1',
          port: port,
          clientId: 'integration-test',
        );

        await client.connect(keepAlive: const Duration(seconds: 5));

        final messageFuture = client.messages.first;
        await client.subscribe(
          'integration/topic',
          options: const MqttSubscriptionOptions(qos: MqttQos.atMostOnce),
        );

        // Give the subscription a moment to settle, then publish.
        await Future<void>.delayed(const Duration(milliseconds: 200));
        await client.publish(
          'integration/topic',
          utf8.encode('hello-mqtt5'),
          qos: MqttQos.atMostOnce,
        );

        final message = await messageFuture.timeout(const Duration(seconds: 5));
        expect(message.topic, 'integration/topic');
        expect(utf8.decode(message.payload), 'hello-mqtt5');

        await client.disconnect();
      } finally {
        broker.kill();
      }
    });

    test('QoS1 and QoS2 publish round trip through the broker', () async {
      final port = await _freePort();
      final broker = await _startBroker(port);

      try {
        final client = MqttClient(
          host: '127.0.0.1',
          port: port,
          clientId: 'qos-test',
        );

        await client.connect(keepAlive: const Duration(seconds: 5));

        final messages = <dynamic>[];
        final sub = client.messages.listen(messages.add);
        await client.subscribe(
          'qos/topic',
          options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce),
        );
        await Future<void>.delayed(const Duration(milliseconds: 200));

        // QoS 1 publish completes with success.
        final qos1 = await client.publish(
          'qos/topic',
          utf8.encode('one'),
          qos: MqttQos.atLeastOnce,
        );
        expect(qos1.reasonCode.value, 0x00);

        // QoS 2 publish completes with success.
        final qos2 = await client.publish(
          'qos/topic',
          utf8.encode('two'),
          qos: MqttQos.exactlyOnce,
        );
        expect(qos2.reasonCode.value, 0x00);

        await _waitFor(
          () => messages.length >= 2,
          timeout: const Duration(seconds: 5),
        );
        final payloads = messages.map((m) => utf8.decode(m.payload)).toList();
        expect(payloads, containsAll(['one', 'two']));

        await sub.cancel();
        await client.disconnect();
      } finally {
        broker.kill();
      }
    });

    test('reconnect after broker restart', () async {
      final port = await _freePort();
      var broker = await _startBroker(port);

      try {
        final client = MqttClient(
          host: '127.0.0.1',
          port: port,
          clientId: 'reconnect-test',
          reconnectManager: _fastReconnect(),
        );

        final states = <String>[];
        client.stateStream.listen((s) => states.add(s.name));

        await client.connect();
        expect(client.state.name, 'connected');

        // Kill the broker; the client should detect the loss and reconnect.
        broker.kill();
        broker = await _startBroker(port);

        await _waitFor(() => client.state.name == 'connected',
            timeout: const Duration(seconds: 10));
      } finally {
        broker.kill();
      }
    });
  });
}

ReconnectManager _fastReconnect() => ReconnectManager(
      initialDelay: const Duration(milliseconds: 100),
      maxDelay: const Duration(milliseconds: 300),
      jitterFactor: 0,
    );

Future<Process> _startBroker(int port) async {
  final dir = await Directory.systemTemp.createTemp('mqtt5-test');
  final config = File('${dir.path}/mosquitto.conf');
  await config.writeAsString('''
listener $port 127.0.0.1
allow_anonymous true
''');

  final process = await Process.start(
    _mosquitto,
    ['-c', config.path],
    mode: ProcessStartMode.normal,
  );

  // Wait for readiness by polling the port.
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (true) {
    try {
      final socket = await Socket.connect('127.0.0.1', port,
          timeout: const Duration(milliseconds: 200));
      socket.destroy();
      break;
    } on SocketException {
      if (DateTime.now().isAfter(deadline)) {
        process.kill();
        fail('mosquitto did not start in time');
      }
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
  }
  return process;
}

Future<int> _freePort() async {
  final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final port = server.port;
  await server.close();
  return port;
}

Future<void> _waitFor(
  bool Function() condition, {
  required Duration timeout,
}) async {
  final deadline = DateTime.now().add(timeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('Timed out waiting for condition');
    }
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }
}
