# mqtt5

A pure-Dart MQTT 5.0 client library. No Flutter, no third-party MQTT
dependencies — just `dart:io` sockets, `SecureSocket` for TLS, and a
spec-first protocol engine.

## Design goals

- MQTT 5.0 first-class: the full property system, all 15 control packets,
  every reason code, QoS 0/1/2 state machines, session resume.
- Codec, transport, protocol state machine and public API are separate layers.
- Correct over fast: protocol behaviour is implemented per the
  [OASIS MQTT 5.0 specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html),
  not from memory.
- Long-running stability: automatic reconnect, half-open TCP detection via
  keep alive, and fuzz/fragmentation-tested incremental decoding.

## Supported platforms

Any platform supported by `dart:io`: Linux, macOS, Windows, Android and iOS
(native). Web is not supported because `dart:io` sockets are unavailable; the
transport layer is an interface so a WebSocket transport can be added.

## Quick start

```dart
import 'dart:convert';
import 'package:mqtt5/mqtt5.dart';

final client = MqttClient(
  clientId: 'cmd-001',
  host: '192.168.1.100',
  port: 1883,
);

client.messages.listen((message) {
  print('${message.topic}: ${utf8.decode(message.payload)}');
});

await client.connect(
  cleanStart: false,
  keepAlive: const Duration(seconds: 30),
  sessionExpiryInterval: const Duration(hours: 1),
);

await client.subscribe(
  'device/+/status',
  options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce),
);

await client.publish(
  'device/U1-001/action',
  utf8.encode('{"action":"pause"}'),
  qos: MqttQos.atLeastOnce,
  properties: [
    const UserProperty('traceId', '123'),
  ],
);

await client.disconnect();
```

## MQTT 5 feature matrix

| Feature | Status |
| --- | --- |
| CONNECT / CONNACK (all properties) | ✅ Supported |
| Clean Start / Session Expiry Interval | ✅ Supported |
| Session Present / session resume | ✅ Supported |
| QoS 0 / 1 / 2 publish and subscribe | ✅ Supported |
| PUBACK/PUBREC/PUBREL/PUBCOMP state machines | ✅ Supported |
| Incoming QoS 2 de-duplication | ✅ Supported |
| Automatic reconnect (backoff + jitter) | ✅ Supported |
| Retransmit unacknowledged messages on resume (DUP) | ✅ Supported |
| Automatic re-subscribe per session semantics | ✅ Supported |
| Keep Alive / PINGREQ / PINGRESP | ✅ Supported |
| Server Keep Alive override | ✅ Supported |
| Half-open TCP detection | ✅ Supported |
| Last Will and Testament | ✅ Supported |
| Enhanced authentication (AUTH) | ✅ Supported |
| Receive Maximum (outgoing flow control) | ✅ Supported |
| Maximum Packet Size (send and receive) | ✅ Supported |
| Maximum QoS | ✅ Supported |
| Retain Available | ✅ Supported |
| Topic Alias (both directions) | ✅ Supported |
| Subscription Identifier | ✅ Supported |
| Shared / Wildcard subscriptions (with capability checks) | ✅ Supported |
| User Properties | ✅ Supported |
| Request / Response properties | ✅ Supported |
| All MQTT 5 reason codes | ✅ Supported |
| Server redirection (Use another server / Server moved) | ✅ Exposed |
| TLS (CA, client cert, SNI, ALPN) | ✅ Supported |
| Runtime metrics | ✅ Supported |
| WebSocket transport | ⏳ Planned (transport is pluggable) |

## QoS semantics

- `publish(..., qos: MqttQos.atMostOnce)` completes once the packet is written.
- `publish(..., qos: MqttQos.atLeastOnce)` completes when PUBACK arrives.
- `publish(..., qos: MqttQos.exactlyOnce)` completes when PUBCOMP arrives.

On reconnect the client never re-sends acknowledged messages; unacknowledged
PUBLISH packets are retransmitted with `DUP=1` and outstanding PUBREL packets
are retransmitted, exactly as required by the session semantics.

## Session and reconnect

```dart
await client.connect(
  cleanStart: false,
  keepAlive: const Duration(seconds: 30),
  sessionExpiryInterval: const Duration(hours: 1),
);
```

With `cleanStart: false` the broker resumes the session if it is still alive.
The client restores in-flight QoS 1/2 state and does not re-subscribe. If the
session was lost, in-flight publishes fail and known subscriptions are
re-established automatically.

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

## Enhanced authentication

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

Implement `MqttAuthenticator` to respond to each challenge.

## Troubleshooting

- Use `MqttClient(logger: PrintLogger(minimumLevel: MqttLogLevel.debug))` for
  connection, QoS state, reconnect and PING diagnostics.
- `client.metrics` exposes bytes, packets, messages, reconnect count and PING
  RTT.
- `client.state` / `client.stateStream` expose the connection lifecycle.

## Limitations

- WebSocket transport is not implemented yet.
- The client does not automatically follow `Server Reference` redirects; it
  surfaces them via `MqttServerMovedException` (on connect) and the
  `onServerMoved` callback (on DISCONNECT).
- TLS session resumption (RFC 5077) is left to the platform TLS stack.

## Development

```bash
dart analyze
dart test                       # unit + property + packet + state machine tests
dart test test/integration      # requires mosquitto at /opt/homebrew/sbin/mosquitto
dart run tool/benchmark.dart    # codec micro-benchmarks
dart run tool/soak_test.dart --host 127.0.0.1 --port 18883 --duration 3600
dart run tool/chaos_test.dart --rounds 20
```
