import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import '../exception/mqtt_exception.dart';
import 'mqtt_transport.dart';

/// Shared plumbing for transports backed by a [dart:io] Socket.
abstract class MqttSocketTransport implements MqttTransport {
  MqttSocketTransport({
    required this.host,
    required this.port,
    this.timeout = const Duration(seconds: 10),
    this.sourceAddress,
  });

  final String host;
  final int port;
  final Duration timeout;
  final String? sourceAddress;

  Socket? _socket;
  final StreamController<Uint8List> _incoming =
      StreamController<Uint8List>.broadcast(sync: true);
  bool _connected = false;
  bool _closed = false;

  @override
  Stream<Uint8List> get incoming => _incoming.stream;

  @override
  bool get isConnected => _connected;

  @override
  Future<void> connect() async {
    final socket = await openSocket();
    try {
      socket.setOption(SocketOption.tcpNoDelay, true);
    } on SocketException {
      // tcpNoDelay is best-effort.
    }
    _socket = socket;
    _connected = true;
    _closed = false;
    socket.listen(
      _onData,
      onError: _onError,
      onDone: _onDone,
      cancelOnError: false,
    );
  }

  Future<Socket> openSocket();

  void _onData(Uint8List data) {
    if (_closed) {
      return;
    }
    _incoming.add(data);
  }

  void _onError(Object error) {
    if (_closed) {
      return;
    }
    _connected = false;
    _incoming.addError(error);
  }

  void _onDone() {
    if (_closed) {
      return;
    }
    _connected = false;
    _incoming.addError(
      MqttTransportException('Connection closed by peer'),
    );
  }

  @override
  void add(Uint8List data) {
    final socket = _socket;
    if (socket == null || !_connected) {
      throw MqttTransportException('Transport is not connected');
    }
    socket.add(data);
  }

  @override
  Future<void> flush() async {
    await _socket?.flush();
  }

  @override
  Future<void> close() async {
    _closed = true;
    _connected = false;
    final socket = _socket;
    _socket = null;
    if (socket != null) {
      try {
        await socket.close();
      } on IOException {
        // Best-effort close.
      }
    }
    await _incoming.close();
  }
}
