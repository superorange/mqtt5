import 'dart:async';
import 'dart:typed_data';

import '../exception/mqtt_exception.dart';
import 'mqtt_transport.dart';

/// An in-memory transport used for tests and as the basis for mock brokers.
///
/// [close] is a local teardown (the incoming stream is closed with done).
/// To simulate a peer drop, call [injectError] or [injectDone] — the same
/// events a real socket surfaces through `onError`.
final class MemoryTransport implements MqttTransport {
  final StreamController<Uint8List> _incoming =
      StreamController<Uint8List>.broadcast(sync: true);
  final List<Uint8List> _outgoing = <Uint8List>[];
  bool _connected = false;
  bool _closed = false;

  /// Whether [close] was called and the transport cannot be reused.
  bool get isClosed => _closed;

  /// Bytes written by the client (what the peer would receive).
  List<Uint8List> get outgoing => List.unmodifiable(_outgoing);

  /// Returns and clears all queued outgoing chunks.
  List<Uint8List> takeOutgoing() {
    final result = List<Uint8List>.from(_outgoing);
    _outgoing.clear();
    return result;
  }

  /// Concatenates all queued outgoing chunks into a single buffer.
  Uint8List takeOutgoingBytes() {
    final total = _outgoing.fold<int>(0, (sum, c) => sum + c.length);
    final result = Uint8List(total);
    var offset = 0;
    for (final chunk in _outgoing) {
      result.setRange(offset, offset + chunk.length, chunk);
      offset += chunk.length;
    }
    _outgoing.clear();
    return result;
  }

  /// Simulates bytes arriving from the peer.
  void inject(Uint8List data) {
    _incoming.add(data);
  }

  /// Simulates a connection failure by emitting [error] to the incoming
  /// stream, as a real transport would on socket error.
  void injectError(Object error, [StackTrace? stackTrace]) {
    if (_closed) {
      return;
    }
    _connected = false;
    _incoming.addError(error, stackTrace ?? StackTrace.current);
  }

  /// Simulates a clean peer FIN: the socket reports an error rather than
  /// just closing the stream, matching [MqttSocketTransport].
  void injectDone() {
    if (_closed) {
      return;
    }
    _connected = false;
    _incoming.addError(
      MqttTransportException('Connection closed by peer'),
      StackTrace.current,
    );
  }

  @override
  Stream<Uint8List> get incoming => _incoming.stream;

  @override
  Future<void> connect() async {
    if (_closed) {
      throw StateError('Cannot reconnect a closed MemoryTransport');
    }
    _connected = true;
  }

  @override
  void add(Uint8List data) {
    if (!_connected || _closed) {
      throw MqttTransportException('Transport is not connected');
    }
    _outgoing.add(data);
  }

  @override
  Future<void> flush() async {}

  @override
  Future<void> close() async {
    _connected = false;
    _closed = true;
    await _incoming.close();
  }

  @override
  bool get isConnected => _connected;
}
