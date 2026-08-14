# Examples

Each example is a standalone Dart program that can be run against any
MQTT 5.0 broker (such as [mosquitto](https://mosquitto.org/)).

## Prerequisites

Start a local broker for the plain-TCP examples:

```bash
mosquitto -p 1883
```

Or install one with `brew install mosquitto` on macOS.

## Running

```bash
# Core workflow: connect, subscribe, publish QoS 0/1/2, handle messages.
dart run example/mqtt5_example.dart --host 127.0.0.1 --port 1883

# TLS (mutual TLS optional).
dart run example/tls_example.dart --host broker.example.com --port 8883 \
  --ca ca.pem --cert client.pem --key client.key

# Enhanced authentication (AUTH exchange).
dart run example/enhanced_auth_example.dart --host 127.0.0.1 --port 1883
```

## What each example shows

| Example | Demonstrates |
| --- | --- |
| `mqtt5_example.dart` | Last Will, session state stream, subscription options, subscription identifiers, QoS 0/1/2 publish semantics, unsubscribe, metrics, graceful disconnect. |
| `tls_example.dart` | `SecureSocket` via `TlsTransport`, CA verification, client certificates, ALPN, bad-certificate callback. |
| `enhanced_auth_example.dart` | `MqttAuthenticator` challenge/response, `authenticationMethod` / `authenticationData` in CONNECT, AUTH packet flow. |
