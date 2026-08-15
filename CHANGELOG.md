## 0.2.0

Bug fixes, almost all of them in the connection lifecycle rather than the
protocol engine. If you are on 0.1.0 it is worth upgrading: several of these
could hang or crash a long-running app. Everything below has a test.

The worst one first. A malformed packet from the broker (PUBLISH with DUP set
on QoS 0, a reserved QoS value, a reserved Retain Handling value) came out of
the decoder as an `ArgumentError` or `RangeError`, and the inbound handler
only caught `MqttException`, so it escaped and took the enclosing zone with
it. One bad packet from a broken or hostile broker was enough to kill the app.
Decoding now produces `MqttMalformedPacketException`, and anything thrown
while decoding or dispatching is treated as a protocol error: the client sends
a DISCONNECT with the matching reason code, closes the connection and
reconnects.

Two ways the client could get stuck:

- A CONNACK rejection left the connect loop still marked as running and never
  released the socket, so the transport leaked and every later `connect()`
  quietly did nothing.
- `disconnect()` never failed the futures that were waiting on an
  acknowledgement, so in-flight QoS 1/2 publishes and pending
  subscribe/unsubscribe calls waited forever. They now fail with
  `MqttConnectionException`, and there is a real timeout on top of that (see
  below).

Fatal errors on a *reconnect* had nowhere to go, since the reconnect paths
start the connect loop without a caller. They escaped as unhandled
asynchronous errors and the client died silently. There is now an
`MqttClient.errors` stream for them.

Other fixes:

- `disconnect()` before `connect()` threw `LateInitializationError`.
- Server capabilities (Maximum QoS, Retain Available, Topic Alias Maximum and
  friends) leaked from one connection into the next. They are reset per
  CONNACK now, so a property the new broker did not send falls back to the
  spec default instead of keeping the old value.
- An exception thrown by a `messages` listener unwound into the socket event
  handler and got reported as a protocol error. The stream is no longer
  synchronous.
- Packet identifiers were released twice, once by the acknowledgement handler
  and once by the publish call. In the wrong interleaving that frees an
  identifier already handed out to another message.
- A Topic Alias was recorded before the PUBLISH carrying the full topic name
  had been written. If that write failed, every later publish to the topic
  referenced an alias the broker had never seen. It is recorded after a
  successful write now.
- Transport teardown was not awaited before the connect loop restarted, so a
  new transport could be thrown away by the previous teardown.
- A DISCONNECT from the broker was only logged. The connection is closed now,
  and reason codes like Server Moved or Banned stop the client rather than
  looping on reconnect.
- `subscribeAll` threw away the filters a broker accepted whenever it rejected
  any other filter in the same SUBACK. The accepted ones are kept, and will be
  re-established after a session loss. SUBACK and UNSUBACK reason code counts
  are checked against the number of filters sent.
- The Assigned Client Identifier from CONNACK was ignored. It is used for
  reconnects now and readable as `MqttClient.effectiveClientId`.
- Maximum Packet Size checks on inbound packets left out the fixed header.
- A `keepAlive` over 65535 seconds produced a confusing error from deep inside
  the packet encoder.

New:

- `autoReconnect` (default `true`). There was previously no way to turn
  reconnection off. With `false`, `connect()` fails on the first unsuccessful
  attempt and a dropped connection is not retried.
- `MqttClient.errors` and `MqttClient.close()`, the latter releasing the
  message, state and error streams.
- `MqttClient.effectiveClientId`.
- The `authenticating` and `disconnecting` connection states are actually
  emitted now, having been declared but never used.

Breaking changes:

- `publish`, `subscribe` and `unsubscribe` give up after `operationTimeout`
  (30 seconds by default) and throw `MqttTimeoutException`. Pass
  `Duration.zero` to wait forever like 0.1.0 did.
- The barrel library no longer exports internals: `ConnectionManager`,
  `FlowController`, `KeepAliveManager`, `PacketIdentifierPool`, `MqttSession`
  and the session stores. `MemoryTransport` moved to
  `package:mqtt5/testing.dart`.
- `MqttQos.fromValue` and `MqttSubscriptionOptions.fromByte` throw
  `MqttMalformedPacketException` rather than `ArgumentError`/`RangeError`.
- `publish` rejects wildcards and empty topic names locally instead of letting
  the broker close the connection over it.
- Dropped the placeholder `bin/mqtt5.dart`.

## 0.1.0

- Initial release: MQTT 5.0 client with QoS 0/1/2, session resume,
  Receive Maximum flow control, Topic Alias, enhanced authentication,
  keep alive, automatic reconnect, TLS, and runtime metrics.
