## 0.4.0

A conformance pass against the OASIS MQTT 5.0 specification, then a second pass
over the connection layer. Everything below is checked against the normative
statement it names, and everything has a test in
`test/spec_conformance_test.dart` or `test/cross_audit_fixes_test.dart`.

Two ways a client could be stranded by an otherwise healthy broker.

Packets that arrive in the same TCP segment as CONNACK are held until session
capabilities are applied, and the hold had a cap of 128 packets. CONNACK
completes its handshake through a microtask, so everything behind it in that
segment is buffered synchronously — and a broker is entitled to put a burst
there: the retained messages for a wildcard subscription, or the backlog of a
resumed session. Both are normal, both overflow 128, and the overflow closed
the connection with DISCONNECT `0x82`. With `autoReconnect` the broker then
replayed the same burst into the same failure. The cap is now a byte budget,
which is what it was meant to be.

Maximum Packet Size from CONNACK was kept after the connection that negotiated
it ended. The next attempt's CONNECT — written before any CONNACK of its own —
was policed against a limit the new server never set. A CONNECT over that limit
is fatal and not retried, so a broker advertising a tight limit could stop the
client permanently. It is reset per connection now, as section 3.1.2.11.4
scopes it.

Then the session resume work. Session resume re-sent stored publications one at a time,
waiting for a send quota slot before each. That wait sat inside the connect
loop, which does not report the connection as established until resume
finishes. A broker that resumed a session while advertising a smaller Receive
Maximum than the client had in flight — legal, and what a loaded broker does —
left the client stuck in `reconnecting` permanently. `operationTimeout` did not
help, because publications that time out are deliberately kept for the next
resume. Resume is now a queue drained by available quota: it never waits, and
acknowledgements pull the rest through.

The same code also made PUBREL wait for send quota. MQTT-3.3.4-8 and
MQTT-4.9.0-3 both say only PUBLISH may be delayed for flow control, and the
broker is waiting on that PUBREL to close out the exchange. PUBREL is now sent
immediately and takes no quota, exactly as section 4.9 describes.

Re-sends were grouped by QoS, so a session holding QoS 1 and QoS 2 publications
replayed them in the wrong order. MQTT-4.6.0-1 requires the order the originals
were sent in, and MQTT-4.6.0-4 requires PUBREL packets in the order their
PUBRECs arrived. Both are now tracked and replayed in order.

Protocol errors the client used to ignore:

- CONNECT, CONNACK, SUBSCRIBE, UNSUBSCRIBE and PINGREQ from the broker were
  silently dropped. A client must never receive these (Table 2-1), and a second
  CONNACK is forbidden by MQTT-3.2.0-2. They now close the connection with
  DISCONNECT `0x82`.
- Any packet arriving before CONNACK was queued and replayed. MQTT-3.2.0-1
  requires CONNACK to be the first packet from the server, so it now fails the
  connect instead.
- A CONNACK reporting Session Present after a Clean Start connection was
  accepted. MQTT-3.2.2-2 forbids the server sending it and MQTT-3.2.2-4 requires
  the client to close the connection; it now does.
- An AUTH packet that changes the Authentication Method, or arrives when the
  CONNECT carried none, is now a protocol error (MQTT-4.12.0-5, MQTT-4.12.0-6).
  The client's own AUTH replies always carry reason code `0x18` and the agreed
  method, per MQTT-4.12.0-3 and MQTT-4.12.0-5; the previous code echoed `0x19`
  back at the server, which only a client may send.

Packet identifiers were released from two places on the acknowledgement path
and again by the call that was unwinding behind it. With the pool exhausted and
a caller blocked on it, the second release handed back an identifier that
caller had already been given, putting two live messages on one identifier.
Identifiers are now released exactly where their entry leaves the session
store. An acknowledgement for an identifier the client does not know is logged
and dropped rather than freeing whatever else is using it.

Topic Alias kept a reverse mapping for the previous topic when an alias was
rebound. Outgoing that never triggered, because a bound alias was never reused;
incoming it was growth a broker could drive without bound by cycling one alias
through many topic names.

Other fixes:

- Topic Names and Topic Filters are validated locally against sections 4.7 and
  4.8.2 — `#` placement, `+` occupying a whole level, `$share/{ShareName}/{filter}`
  structure, length and the null character — for `publish`, `subscribe`,
  `unsubscribe` and the will topic. A malformed filter used to reach the broker
  and come back as a disconnection.
- A Response Topic containing a wildcard is rejected (MQTT-3.3.2-14).
- A Topic Alias supplied by the caller is checked against the Topic Alias
  Maximum the broker announced, instead of being sent and rejected.
- Setting a non-zero Session Expiry Interval on DISCONNECT when CONNECT asked
  for zero is rejected locally (section 3.14.2.2.2).
- `MqttReasonCode` was missing `0x01` Granted QoS 1 and `0x02` Granted QoS 2,
  so `tryFromValue` returned null for two thirds of the SUBACK success codes.
- `TlsTransport` read PEM material with `String.codeUnits`, which mangles any
  non-ASCII byte. It uses UTF-8 now.
- A fatal protocol error now sends DISCONNECT with the matching reason code
  before closing, rather than dropping the socket silently.
- A PUBLISH whose Topic Name contains `+` or `#` is rejected on decode
  (MQTT-3.3.2-2). The send path already checked this; the receive path did not.
  An empty Topic Name is still accepted — that is how a Topic Alias is used.
- A CONNACK carrying a non-zero reason code together with Session Present is
  rejected. MQTT-3.2.2-6 requires the server to clear Session Present in that
  case, and the combination claims the connection both failed and resumed.
- Passing a property to `connect()` that one of its named arguments also emits
  — `sessionExpiryInterval`, `receiveMaximum`, `maximumPacketSize`,
  `topicAliasMaximum`, `authenticationMethod`, `authenticationData` — now
  throws `ArgumentError`. It used to fail inside the encoder as a duplicate
  property, which was reported as a protocol error, classified as not
  retryable, and ended the connection loop over a mistake at the call site.
  Either spelling on its own still works.
- A Topic Alias supplied by the caller is now recorded in the outgoing alias
  map. The PUBLISH establishing it carried the full topic name and the alias,
  which is correct, but the mapping was never kept: every later publish to that
  topic sent the full name again, and the alias could be handed to a different
  topic.
- The automatic re-subscribe after a session loss released its packet
  identifier when it timed out. `subscribeAll` deliberately holds one in that
  case (MQTT-2.2.1-4); the two paths now agree.
- `FlowController` woke every blocked publisher on each freed send quota slot,
  so N waiters cost O(N) wakeups per release. It hands the slot to the
  longest-waiting caller instead. Ordering was already correct and is unchanged.

New:

- `MqttClient.reauthenticate()` performs the client-initiated re-authentication
  of section 4.12.1. Publishing and subscribing continue while it runs, as that
  section requires.
- `MqttClient.serverCapabilities` exposes everything the broker announced in
  CONNACK, including the Session Expiry Interval and Server Keep Alive it chose.
  A granted Session Expiry Interval is reported, not adopted: the next CONNECT
  still asks for what the application configured.
- `MqttTopic` exposes the topic name and filter rules for callers that want to
  validate before calling.
- `MqttTopic.matches(filter, topic)` answers whether a topic matches a filter,
  including `+`, `#`, the `$`-prefix rule of section 4.7.2 and `$share/`
  filters. Messages all arrive on one stream, so routing them needed this.
- `pingResponseTimeout` on the constructor sets how long a PINGREQ may go
  unanswered, separately from `keepAlive`. The default is unchanged — one keep
  alive interval — which means a silently dropped link takes two intervals to
  notice: one for the idle timer, one for the reply that never comes. That is
  fine at 60 seconds and poor at 300. Setting it decouples how often an idle
  connection must produce traffic from how fast a dead one is noticed.
- `reconnectCleanStart` on `connect()` sets the Clean Start used for reconnects
  only. Without it, a reconnect now sends `cleanStart: false` when a non-zero
  Session Expiry Interval was requested, which is what asking for a session to
  outlive the connection means (MQTT-3.1.2-4, MQTT-3.1.2-5).
- `topicAliasEviction` on the constructor lets the outgoing alias map evict its
  least recently used alias once every slot is bound, so a client publishing to
  more topics than the broker allows aliases for can keep aliasing. It is off
  by default: rebinding costs a full topic name, so it only pays when topics
  repeat.
- A SUBACK that arrives after `subscribe` has timed out still registers the
  subscriptions the broker granted, so a later re-subscribe reproduces them.

Breaking changes:

- `publish`, `subscribe`, `unsubscribe` and `MqttWill` throw `ArgumentError` for
  topics that were previously passed through to the broker.
- Encoding or decoding a Response Topic with a wildcard throws
  `MqttProtocolException`.
- A broker that sends a client-only packet, a second CONNACK, or Session Present
  after a Clean Start now loses the connection instead of being ignored.
- A PUBLISH with a wildcard Topic Name, and a CONNACK pairing a failure reason
  code with Session Present, now throw `MqttProtocolException` on decode.
- Reconnecting after requesting a non-zero Session Expiry Interval now sends
  `cleanStart: false`. Pass `reconnectCleanStart: true` for the old behaviour.
- `connect()` throws `ArgumentError` when a named argument and `properties`
  both set the same CONNECT property.
- Removed unused members from internal session types. These were never exported.

Deliberately not changed, having been raised and checked against the
specification:

- Inbound Receive Maximum still counts only QoS 2 exchanges. MQTT-3.3.4-9 scopes
  it to publications "where it has not sent a PUBACK or PUBCOMP in response",
  and QoS 1 is acknowledged in the same turn it is received. The count now
  reads through a named getter, because the previous expression looked like a
  bug to anyone who had not worked that through.
- Subscribing at a QoS above the broker's Maximum QoS is still allowed.
  MQTT-3.2.2-10 requires the server to accept any requested QoS and grant what
  it supports in the SUBACK; Maximum QoS constrains PUBLISH (MQTT-3.2.2-11),
  not SUBSCRIBE.
- A PUBACK, PUBREC or PUBCOMP for an unknown packet identifier is logged, not
  escalated. Section 3.6.2.1 calls that state mismatch expected during recovery.
- Packet identifiers for a timed-out SUBSCRIBE or UNSUBSCRIBE stay reserved.
  They only become reusable once the acknowledgement arrives (MQTT-2.2.1-4);
  losing the connection reclaims them.
- A DISCONNECT reporting an unsupported feature — `0x9A`, `0x9B`, `0x9E`,
  `0xA1`, `0xA2` — is still retried. Treating it as fatal would stop the client
  for good over what may be a momentary refusal, and backoff already caps the
  retry rate.
- Session state still lives only in memory, and there is still no WebSocket
  transport. Both need an API, not a fix.

## 0.3.0

This release focuses on connection failures and reconnect cleanup. The main
case behind it was a TLS handshake error escaping from a background reconnect
and terminating the process.

- TLS handshake and protocol errors are treated as terminal. A background
  failure is reported through `MqttClient.errors` instead of escaping as an
  unhandled asynchronous exception.
- `isRetryableMqttConnectionError` exposes the same retry decision to wrapper
  libraries, so reconnect policy does not have to duplicate MQTT reason codes.
- Transport errors received while waiting for CONNACK fail the connection
  immediately instead of waiting for `connackTimeout`.
- Connection-loss handling is serialized so duplicate socket error/done events
  cannot start overlapping teardown or reconnect work.
- `disconnect()` cancels a pending reconnect delay and waits for the old
  connection loop to finish before another `connect()` can start.
- Transport listener cancellation and socket close failures are contained and
  logged instead of escaping from background cleanup.
- With `autoReconnect: false`, a connection lost after startup is also emitted
  on `MqttClient.errors`.
- Socket transports emit only one terminal event and keep the original stack
  trace when forwarding an error.
- Fatal disconnect errors are passed to pending publish, subscribe and
  unsubscribe operations.
- Resetting an exhausted packet identifier pool now wakes blocked allocators.
- Invalid port, timeout, reconnect, CONNECT and TLS client-certificate settings
  are rejected before network I/O.
- `MqttException.toString()` now includes the concrete exception type and its
  underlying cause, which makes TLS and transport logs more useful.
- `PrintLogger.minimumLevel` now keeps messages at that severity and above;
  warning and error messages were previously filtered in the wrong direction.
- Exceptions thrown by an application-provided logger are contained; logging
  can no longer interrupt packet handling, cleanup, or reconnect work.
- Added regression tests for handshake failures, terminal reconnect failures,
  reconnect cancellation, cleanup errors and packet identifier reset.
- Reworked the README to keep the setup, TLS notes and runtime behavior easier
  to find.

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
