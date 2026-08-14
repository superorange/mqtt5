import 'dart:typed_data';

/// A byte-oriented MQTT transport.
///
/// The codec layer only ever talks to this interface, so transports for
/// TCP, TLS, WebSocket or in-memory test channels are interchangeable.
abstract interface class MqttTransport {
  /// Chunks of bytes received from the peer.
  Stream<Uint8List> get incoming;

  /// Establishes the underlying connection.
  Future<void> connect();

  /// Queues [data] for sending to the peer.
  void add(Uint8List data);

  /// Flushes any buffered outbound data.
  Future<void> flush();

  /// Closes the transport.
  Future<void> close();

  /// Whether the transport is currently connected.
  bool get isConnected;
}
