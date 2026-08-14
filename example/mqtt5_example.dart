// A comprehensive example showing the core MQTT 5.0 client workflow:
// connecting, subscribing (with options + subscription identifier),
// publishing at QoS 0/1/2, handling incoming messages and disconnecting.
//
// Run against a local broker (e.g. mosquitto):
//
//   mosquitto -p 1883
//   dart run example/mqtt5_example.dart --host 127.0.0.1 --port 1883
//
// Or point it at any MQTT 5 broker:
//
//   dart run example/mqtt5_example.dart --host broker.example.com --port 1883
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:mqtt5/mqtt5.dart';

Future<void> main(List<String> args) async {
  final options = _parseArgs(args);

  final client = MqttClient(
    host: options.host,
    port: options.port,
    clientId: options.clientId,
    // Replace with PrintLogger(minimumLevel: MqttLogLevel.debug) to see
    // CONNECT/CONNACK, QoS state transitions, reconnect and PING activity.
    logger: const SilentLogger(),
    // The Last Will is published by the broker if this client dies without
    // sending a DISCONNECT packet.
    will: MqttWill(
      topic: 'example/will',
      payload: utf8.encode('$options.clientId went offline unexpectedly'),
      qos: MqttQos.atLeastOnce,
    ),
  );

  // Track the connection lifecycle.
  final stateSub = client.stateStream.listen((state) {
    print('[state] ${state.name}');
  });

  // Handle incoming application messages.
  final messageSub = client.messages.listen((message) {
    print('[message] topic=${message.topic} '
        'qos=${message.qos.value} '
        'retain=${message.retain} '
        'dup=${message.duplicate} '
        'subscriptionIds=${message.subscriptionIdentifiers} '
        'payload=${utf8.decode(message.payload)}');
  });

  // Report metrics every few seconds while running.
  final metricsTimer = Timer.periodic(const Duration(seconds: 5), (_) {
    final m = client.metrics;
    print('[metrics] state=${client.state.name} '
        'sent=${m.bytesSentB} received=${m.bytesReceivedB} '
        'inflight=${client.inflightCount} '
        'reconnects=${m.reconnectCount} pingRtt=${m.lastPingRtt}');
  });

  try {
    // cleanStart: false + a session expiry interval lets the broker resume
    // the session after a reconnect.
    await client.connect(
      cleanStart: false,
      keepAlive: const Duration(seconds: 30),
      sessionExpiryInterval: const Duration(hours: 1),
    );
    print('connected (sessionPresent=${client.sessionPresent})');

    // Subscribe to a wildcard filter with QoS 1 and a subscription
    // identifier. The identifier is echoed back in matching messages.
    await client.subscribe(
      'example/+/status',
      options: const MqttSubscriptionOptions(
        qos: MqttQos.atLeastOnce,
        noLocal: false,
        retainAsPublished: true,
        retainHandling: MqttRetainHandling.sendAtSubscribe,
      ),
      subscriptionIdentifier: 42,
    );

    // Subscribe to a second filter to show multiple subscriptions.
    await client.subscribe('example/replies');

    // QoS 0: fire and forget; the future completes once written.
    await client.publish(
      'example/device-1/status',
      utf8.encode('{"state":"idle"}'),
      qos: MqttQos.atMostOnce,
    );

    // QoS 1: completes when PUBACK arrives from the broker.
    final qos1 = await client.publish(
      'example/device-1/status',
      utf8.encode('{"state":"running"}'),
      qos: MqttQos.atLeastOnce,
      properties: const [UserProperty('traceId', 'qos1-demo')],
    );
    print('QoS1 publish acknowledged: ${qos1.reasonCode.name}');

    // QoS 2: completes when the full PUBLISH/PUBREC/PUBREL/PUBCOMP exchange
    // has finished (exactly-once delivery).
    final qos2 = await client.publish(
      'example/device-2/status',
      utf8.encode('{"state":"done"}'),
      qos: MqttQos.exactlyOnce,
    );
    print('QoS2 publish completed: ${qos2.reasonCode.name}');

    // Give incoming messages a moment to arrive before disconnecting.
    await Future<void>.delayed(const Duration(seconds: 1));

    await client.unsubscribe(['example/replies']);
    print('unsubscribed from example/replies');
  } on MqttException catch (e) {
    print('MQTT error: $e');
  } finally {
    await client.disconnect(
      reasonCode: MqttReasonCode.success,
      properties: const [UserProperty('shutdown', 'clean')],
    );
    print('disconnected');

    metricsTimer.cancel();
    await stateSub.cancel();
    await messageSub.cancel();
  }
}

({String host, int port, String clientId}) _parseArgs(List<String> args) {
  var host = '127.0.0.1';
  var port = 1883;
  var clientId = 'example-${DateTime.now().millisecondsSinceEpoch}';

  for (var i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--host':
        host = args[++i];
      case '--port':
        port = int.parse(args[++i]);
      case '--client-id':
        clientId = args[++i];
      case '--help':
        print('Usage: dart run example/mqtt5_example.dart '
            '[--host <host>] [--port <port>] [--client-id <id>]');
        exit(0);
    }
  }

  return (host: host, port: port, clientId: clientId);
}
