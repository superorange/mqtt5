# mqtt5

A pure Dart MQTT 5 client. I wrote it for device-side tools where I needed
MQTT 5 but did not want to pull in Flutter or another MQTT implementation.

It uses `dart:io`, so it works on desktop and mobile, but not on the web.
WebSocket transport is not implemented yet.

## Usage

```bash
dart pub add mqtt5
```

```dart
import 'dart:convert';
import 'package:mqtt5/mqtt5.dart';

final client = MqttClient(
  host: '192.168.1.100',
  clientId: 'cmd-001',
);

client.messages.listen((message) {
  print('${message.topic}: ${utf8.decode(message.payload)}');
});

await client.connect(keepAlive: const Duration(seconds: 30));

await client.subscribe(
  'device/+/status',
  options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce),
);

await client.publish(
  'device/U1-001/action',
  utf8.encode('{"action":"pause"}'),
  qos: MqttQos.atLeastOnce,
);

await client.close();
```

`disconnect()` keeps the client reusable. `close()` closes the streams as well,
so do not use that client again after closing it.

## A few behavior notes

- QoS 0 completes after writing the packet. QoS 1 waits for PUBACK and QoS 2
  waits for PUBCOMP.
- Publish, subscribe and unsubscribe time out after 30 seconds by default. This
  is controlled by `operationTimeout`; `Duration.zero` disables the timeout.
- Reconnect is enabled by default and uses backoff. Set `autoReconnect: false`
  if the application should own the reconnect loop.
- Use `cleanStart: false` and a non-zero `sessionExpiryInterval` when the broker
  session should survive a reconnect.
- Errors after the initial connection are reported on `client.errors` because
  there is no longer a pending `connect()` call to throw them to.

For example:

```dart
client.errors.listen((error) {
  print('MQTT error: $error');
});
```

TLS failures, bad credentials and other permanent broker rejections stop the
reconnect loop. Change the configuration and call `connect()` again.

## TLS

For a broker using a public certificate, `useTls: true` is normally enough:

```dart
final client = MqttClient(
  host: 'broker.example.com',
  port: 8883,
  useTls: true,
);
```

For a private CA or mutual TLS:

```dart
final client = MqttClient(
  host: 'broker.example.com',
  port: 8883,
  useTls: true,
  securityContext: TlsTransport.createSecurityContext(
    trustedCertificates: caPem,
    certificateChain: clientCertPem,
    privateKey: clientKeyPem,
  ),
);
```

`onBadCertificate` is available for unusual setups. Do not return `true` for
every certificate in production unless you are deliberately giving up server
identity checking.

## Supported bits

The usual MQTT 5 features are there: QoS 0/1/2, retained messages, wills,
properties, topic aliases, subscription identifiers, session resume, receive
maximum, maximum packet size and enhanced authentication.

Session state only lives in memory. It survives a reconnect, not a process
restart. `Server Reference` is reported through `onServerMoved`; the client
does not follow it automatically.

## Debugging

```dart
final client = MqttClient(
  host: 'broker.example.com',
  logger: PrintLogger(minimumLevel: MqttLogLevel.debug),
);
```

There is also `stateStream` for connection changes and `metrics` for packet,
message, reconnect and protocol error counters.

## Development

```bash
dart test
dart test test/integration  # requires mosquitto
dart run tool/benchmark.dart
dart run tool/soak_test.dart --host 127.0.0.1 --port 18883 --duration 3600
dart run tool/chaos_test.dart --rounds 20
```
