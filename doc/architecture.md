# Architecture

The library is layered so that transport, wire format and protocol state are
strictly separated:

```
MqttTransport          (dart:io Socket / SecureSocket / MemoryTransport)
    ↓  Stream<Uint8List>
MqttPacketDecoder      (incremental, handles TCP packet fragmentation)
    ↓  MqttPacket
MqttPacketCodec        (fixed header + all 15 control packets)
    ↓  typed packets
PropertyCodec          (declarative MQTT 5 property metadata)
    ↓
ConnectionManager      (handshake, keep alive, reconnect loop)
    ↓
MqttClient             (public API: connect/disconnect/subscribe/publish)
```

## Key components

- `lib/src/codec/` — `MqttReader`/`MqttWriter`, `VariableByteInteger`,
  `MqttUtf8`, `ByteAccumulator`, `MqttPacketDecoder`. The decoder never
  assumes a socket chunk equals a packet; it handles partial packets,
  coalesced packets and Remaining Length spanning chunks.
- `lib/src/packet/` — one file per control packet plus the shared
  `MqttPacketCodec`. Fixed-header flags are strictly validated.
- `lib/src/property/` — 27 strongly-typed properties driven by declarative
  metadata (identifier, type, allowed packets, repeatability, validation).
- `lib/src/session/` — session state: packet identifier pool, outgoing QoS 1/2
  stores, incoming QoS 2 de-duplication, subscription store, topic alias maps.
- `lib/src/client/` — `ConnectionManager` (transport lifecycle + handshake +
  reconnect), `KeepAliveManager`, `ReconnectManager`, `FlowController`,
  `MqttClient`.

## State machine boundaries

Three concepts are deliberately distinct:

- **TCP connection** — the transport socket.
- **MQTT connection** — a completed CONNECT/CONNACK exchange.
- **MQTT session** — subscriptions + in-flight state that survives a
  reconnect when the broker reports `sessionPresent`.

`MqttSession` holds the session state; `ConnectionManager` owns the transport
and the reconnect loop. On reconnect with a resumed session, in-flight QoS 1
PUBLISH packets are retransmitted with `DUP=1` and QoS 2 entries are either
retransmitted or their PUBREL is re-sent, based on their state.
