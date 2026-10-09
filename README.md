# mqtt5

An MQTT 5.0 client written in pure Dart.

It runs wherever `dart:io` is available: the Dart VM, and Flutter on Android,
iOS and desktop. Web is not supported.

## Features

- MQTT 5.0 over TCP or TLS, including mutual TLS
- QoS 0, 1 and 2, retained messages and Will
- Session resume and automatic reconnect with backoff
- Receive Maximum, Maximum Packet Size, Topic Alias and Subscription
  Identifier
- Enhanced authentication and re-authentication
- Connection state stream, metrics and a pluggable logger

Not supported: WebSocket transport, persisting session state across restarts.

## Install

```bash
dart pub add mqtt5
```

## Usage

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
client.errors.listen((error) {
  print('MQTT error: $error');
});

await client.connect(keepAlive: const Duration(seconds: 30));

await client.subscribe(
  'device/+/status',
  options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce),
);

final result = await client.publish(
  'device/U1-001/action',
  utf8.encode('{"action":"pause"}'),
  qos: MqttQos.atLeastOnce,
);
if (result.isError) {
  print('Rejected by the broker: ${result.reasonCode}');
}

await client.close();
```

`disconnect()` leaves the client reusable. `close()` also closes its streams;
the client cannot be used afterwards.

## Behavior

### Publishing

- QoS 0 completes once the packet is written, QoS 1 on PUBACK, QoS 2 on
  PUBCOMP.
- A broker rejection is returned as a result, not thrown. Reason codes from
  `0x80` up are errors (`result.isError`); `0x10`, no matching subscribers,
  is not.
- A QoS 1/2 message that has been sent does not time out. It stays in the
  session until the broker acknowledges it. Do not publish it again yourself,
  or the broker may get it twice. If the broker stops acknowledging, the
  client reconnects after `ackTimeout` (60 seconds) and a persistent session
  resends it.
- Subscribe, unsubscribe, and a publish still waiting to be sent, time out
  after `operationTimeout` (30 seconds). `Duration.zero` disables it.

### Sessions and reconnect

- Reconnect is on by default and uses backoff. Set `autoReconnect: false` to
  handle reconnecting yourself.
- To keep the broker session across reconnects, connect with
  `cleanStart: false` and a non-zero `sessionExpiryInterval`.
- Session state is held in memory by the client object. A new `MqttClient`,
  for example after an app restart, cannot resume a session the broker still
  holds: `connect(cleanStart: false)` throws `MqttSessionNotOwnedException`.
  Connect it with `cleanStart: true`. To receive the messages queued while
  offline, pass `adoptBrokerSession: true` and read its documentation for the
  trade-offs.

### Messages and errors

- Messages that arrive before `messages` has a listener are kept, so
  listening after `connect()` loses nothing.
- An exception thrown by a listener is reported on `client.errors`.
- After the first `connect()` has returned, connection failures are reported
  on `client.errors`. Bad credentials, TLS failures and other permanent
  rejections stop the reconnect loop; fix the configuration and call
  `connect()` again.

Upgrading from 0.4: see the breaking changes in `CHANGELOG.md`.

## TLS

For a broker with a publicly trusted certificate:

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

`onBadCertificate` can override certificate validation. Returning `true` for
every certificate turns off server identity checks; do not do that in
production.

A `Server Reference` sent by the broker is reported through `onServerMoved`.
The client does not follow it automatically.

## Debugging

```dart
final client = MqttClient(
  host: 'broker.example.com',
  logger: PrintLogger(minimumLevel: MqttLogLevel.debug),
);
```

`stateStream` reports connection state changes, and `metrics` counts packets,
messages, reconnects and protocol errors.

## Development

```bash
dart test -x real-broker         # unit tests, no broker needed
dart test test/real_broker -j 1  # needs mosquitto 2.x; EMQX tests need Docker
dart run tool/benchmark.dart
dart run tool/soak_test.dart --host 127.0.0.1 --port 18883 --duration 3600
dart run tool/chaos_test.dart --rounds 20
```
