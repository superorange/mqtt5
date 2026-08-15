# mqtt5

An MQTT 5.0 client for Dart, written against the
[OASIS spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
Pure Dart, no Flutter dependency, no third-party MQTT code — just `dart:io`
sockets and `SecureSocket` for TLS.

It runs anywhere `dart:io` runs: Linux, macOS, Windows, Android, iOS. Not the
web, since there are no raw sockets there. The transport is behind an
interface, so a WebSocket transport can be dropped in later.

```bash
dart pub add mqtt5
```

## Getting started

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
  properties: [const UserProperty('traceId', '123')],
);

await client.close();
```

`disconnect()` hangs up but leaves the client reusable, so you can `connect()`
again. `close()` also closes the `messages`, `stateStream` and `errors`
streams and is the end of the road for that client.

## What publish actually waits for

QoS 0 returns as soon as the bytes are written. QoS 1 returns when PUBACK
arrives, QoS 2 when PUBCOMP does. The returned `MqttPublishResult` carries the
broker's reason code and properties, so a broker that accepts a message with
`noMatchingSubscribers` is visible to you rather than silently swallowed.

If nothing comes back within `operationTimeout` (30 seconds by default), the
call throws `MqttTimeoutException`. That applies to `subscribe` and
`unsubscribe` too. Pass `Duration.zero` if you would rather wait forever.

## Sessions and reconnecting

```dart
await client.connect(
  cleanStart: false,
  sessionExpiryInterval: const Duration(hours: 1),
);
```

With `cleanStart: false` the broker resumes your session if it still has it.
In-flight QoS 1/2 messages are retransmitted with DUP set, outstanding PUBRELs
are re-sent, and subscriptions are left alone because the broker still has
them. If the session turned out to be gone, in-flight publishes fail and the
client re-subscribes to everything it knows about.

Dropped connections come back with exponential backoff and jitter. Half-open
TCP connections are caught by keep alive, so a connection that silently died
gets noticed instead of hanging forever.

Turn all of that off with `autoReconnect: false` — then `connect()` throws on
the first failure and a later drop just leaves the client disconnected.

The first `connect()` throws if the broker refuses you. After that there is no
caller left to throw at, so anything fatal on a later reconnect shows up here:

```dart
client.errors.listen((error) {
  // Credentials revoked, broker moved, protocol error. The client has stopped.
});
```

Reason codes that will not get better on a retry — Banned, Not Authorized,
Server Moved, Use Another Server, Bad Authentication Method — stop the client
instead of hammering a broker that keeps saying no.

## MQTT 5 features

All 15 control packets and the full property system are implemented, along
with every reason code in the spec. The parts you are most likely to care
about:

- **Flow control.** Receive Maximum is honoured for outgoing QoS 1/2, so the
  client will not exceed the broker's in-flight window.
- **Topic Alias**, both directions, negotiated from CONNACK.
- **Server capabilities** are enforced locally: publishing at QoS 2 to a
  broker that advertises Maximum QoS 1, or with `retain: true` where retain is
  unavailable, fails immediately instead of getting you disconnected.
- **Subscription Identifiers**, available on received messages via
  `message.subscriptionIdentifiers`.
- **Enhanced authentication** (AUTH), including answering a broker's
  re-authentication challenge mid-session.
- **Last Will and Testament** with all will properties.
- **Maximum Packet Size** in both directions.

WebSocket transport is the notable gap.

## TLS

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

Leave `securityContext` off to use the platform's trusted roots. ALPN and a
custom `onBadCertificate` are available on the constructor.

## Enhanced authentication

Implement `MqttAuthenticator` and answer each challenge the broker sends:

```dart
final client = MqttClient(
  host: 'broker.example.com',
  authenticator: MyScramAuthenticator(),
);

await client.connect(
  authenticationMethod: 'SCRAM-SHA-256',
  authenticationData: initialData,
);
```

## When something is wrong

Turn on logging:

```dart
MqttClient(
  host: 'broker.example.com',
  logger: PrintLogger(minimumLevel: MqttLogLevel.debug),
);
```

`client.state` and `client.stateStream` follow the connection lifecycle, and
`client.metrics` counts bytes, packets, messages, reconnects, protocol errors
and the last PING round trip.

For testing your own code against the client without a broker, there is an
in-memory transport:

```dart
import 'package:mqtt5/testing.dart';

final transport = MemoryTransport();
final client = MqttClient(host: 'x', transportFactory: () => transport);
```

## Things to know

Session state lives in memory. It survives a reconnect, not a process restart.

The client will not chase a `Server Reference` redirect for you. It stops and
reports the new address through `onServerMoved` and `MqttServerMovedException`;
building a client for the new address is your call.

TLS session resumption is whatever the platform TLS stack does on its own.

## Development

```bash
dart test                    # unit, property and state machine tests
dart test test/integration   # needs mosquitto; skipped if it is not installed
dart run tool/benchmark.dart
dart run tool/soak_test.dart --host 127.0.0.1 --port 18883 --duration 3600
dart run tool/chaos_test.dart --rounds 20
```

The integration suite runs against a real mosquitto instance it starts itself.
`tool/chaos_test.dart` kills the broker at random points and checks the client
recovers; `tool/soak_test.dart` is for leaving running overnight.
