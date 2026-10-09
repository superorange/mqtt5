import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:mqtt5/src/client/mqtt_client.dart';
import 'package:mqtt5/src/client/reconnect_manager.dart';
import 'package:mqtt5/src/mqtt_qos.dart';
import 'package:mqtt5/src/packet/connect.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/subscription.dart';
import 'package:test/test.dart';

import '../support/mosquitto_tools.dart';

void main() {
  final mosquitto = mosquittoProgram('mosquitto', directory: 'sbin');
  if (mosquitto == null) {
    // Still declare the group so test output reflects the skip.
    group('mosquitto integration', () {
      test('skipped: mosquitto not installed', () {
        markTestSkipped('mosquitto binary not found; install mosquitto or set '
            'MQTT5_MOSQUITTO_PREFIX');
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

    test('session resume keeps subscription across reconnect', () async {
      final port = await _freePort();
      final broker = await _startBroker(port);

      try {
        final client = MqttClient(
          host: '127.0.0.1',
          port: port,
          clientId: 'session-test',
        );
        final publisher = MqttClient(
          host: '127.0.0.1',
          port: port,
          clientId: 'session-publisher',
        );

        await client.connect(
          cleanStart: false,
          keepAlive: const Duration(seconds: 5),
          sessionExpiryInterval: const Duration(seconds: 30),
        );
        expect(client.sessionPresent, isFalse);

        final messageFuture = client.messages.first;
        await client.subscribe(
          'session/topic',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce),
        );

        // Graceful disconnect keeps the session on the broker.
        await client.disconnect();

        await publisher.connect();
        await publisher.publish(
          'session/topic',
          utf8.encode('while-offline'),
          qos: MqttQos.atLeastOnce,
        );
        await publisher.disconnect();

        await client.connect(
          cleanStart: false,
          keepAlive: const Duration(seconds: 5),
          sessionExpiryInterval: const Duration(seconds: 30),
        );
        expect(client.sessionPresent, isTrue);

        // The queued message must be delivered because the subscription and
        // QoS1 state survived the reconnect.
        final message = await messageFuture.timeout(const Duration(seconds: 5));
        expect(utf8.decode(message.payload), 'while-offline');

        await client.disconnect();
      } finally {
        broker.kill();
      }
    });

    test('last will is published on abnormal disconnect', () async {
      final port = await _freePort();
      final broker = await _startBroker(port);

      try {
        final subscriber = MqttClient(
          host: '127.0.0.1',
          port: port,
          clientId: 'will-subscriber',
        );
        await subscriber.connect();
        final messageFuture = subscriber.messages.first;
        await subscriber.subscribe(
          'will/topic',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce),
        );
        await Future<void>.delayed(const Duration(milliseconds: 200));

        // Connect a raw socket with a Last Will and kill it abruptly.
        final socket = await Socket.connect('127.0.0.1', port);
        final connect = MqttConnectPacket(
          clientId: 'will-client',
          will: MqttWill(
            topic: 'will/topic',
            payload: utf8.encode('client-died'),
            qos: MqttQos.atLeastOnce,
          ),
        );
        socket.add(MqttPacketCodec.encode(connect));
        await socket.first.timeout(const Duration(seconds: 5));
        socket.destroy();

        final message =
            await messageFuture.timeout(const Duration(seconds: 5));
        expect(utf8.decode(message.payload), 'client-died');

        await subscriber.disconnect();
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
    requireMosquittoProgram('mosquitto', directory: 'sbin'),
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
