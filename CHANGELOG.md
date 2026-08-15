## 0.2.0

Connection lifecycle and decoder hardening. All of the fixes below have
regression tests.

### Breaking

- `lib/mqtt5.dart` no longer exports internal machinery (`ConnectionManager`,
  `FlowController`, `KeepAliveManager`, `PacketIdentifierPool`, `MqttSession`
  and the session stores). `MemoryTransport` moved to
  `package:mqtt5/testing.dart`.
- `publish`, `subscribe` and `unsubscribe` now fail with
  `MqttTimeoutException` after `operationTimeout` (30s by default) instead of
  waiting forever. Pass `operationTimeout: Duration.zero` for the old
  behaviour.
- `MqttQos.fromValue` and `MqttSubscriptionOptions.fromByte` throw
  `MqttMalformedPacketException` instead of `ArgumentError`/`RangeError`.
- `publish` rejects wildcards and empty topic names with `ArgumentError`
  rather than letting the broker close the connection.
- The stale `bin/mqtt5.dart` stub was removed.

### Fixed

- A malformed packet from the broker (for example PUBLISH with DUP set on
  QoS 0) escaped the decoder as an `ArgumentError` and took down the enclosing
  zone. Decode failures are now protocol errors: the client sends DISCONNECT
  with the matching reason code, tears the connection down and reconnects.
- A CONNACK rejection wedged the client: the connect loop never reset its
  running flag, so the transport leaked and every later `connect()` silently
  did nothing.
- A rejection or unrecoverable failure on a *reconnect* escaped as an
  unhandled asynchronous error. Such errors are now reported on the new
  `MqttClient.errors` stream.
- `disconnect()` left in-flight QoS 1/2 publishes and pending
  subscribe/unsubscribe futures hanging forever. They are now failed with
  `MqttConnectionException`.
- `disconnect()` before `connect()` threw a `LateInitializationError`.
- Server capabilities (Maximum QoS, Retain Available, Topic Alias Maximum, …)
  from one connection leaked into the next; they are now reset per CONNACK.
- An exception thrown by a `messages` listener unwound into the socket event
  handler and was misreported as a protocol error.
- Packet identifiers were released twice (by the acknowledgement handler and
  by the publish call), which could free an identifier that had already been
  handed out again.
- A Topic Alias was bound before the PUBLISH carrying the full topic name was
  written; if the write failed, every later publish referenced an alias the
  broker had never seen. The binding now happens after a successful write.
- The transport teardown was not awaited before the connect loop restarted,
  so a new transport could be discarded by the previous teardown.
- A DISCONNECT from the broker was only logged; the connection is now torn
  down, and reason codes such as Server Moved or Banned stop the client
  instead of reconnecting in a loop.
- `subscribeAll` discarded the filters a broker accepted when it rejected any
  other filter in the same SUBACK. Accepted filters are now recorded (and
  re-established after a session loss) before the error is thrown.
- SUBACK/UNSUBACK reason code counts are validated against the number of
  topic filters that were sent.
- The Assigned Client Identifier from CONNACK was ignored; it is now used for
  subsequent reconnects and exposed as `MqttClient.effectiveClientId`.
- Maximum Packet Size checks on inbound packets excluded the fixed header.
- `keepAlive` values above 65535 seconds produced an obscure error from the
  packet encoder.

### Added

- `MqttClient.autoReconnect` (default `true`). With `false`, `connect()` fails
  on the first unsuccessful attempt and a dropped connection is not retried.
- `MqttClient.errors`: connection failures with no caller left to throw to.
- `MqttClient.close()`: disconnects and releases the message, state and error
  streams.
- `MqttClient.effectiveClientId`.
- The `authenticating` and `disconnecting` connection states are now emitted.

## 0.1.0

- Initial release: MQTT 5.0 client with QoS 0/1/2, session resume,
  Receive Maximum flow control, Topic Alias, enhanced authentication,
  keep alive, automatic reconnect, TLS, and runtime metrics.
