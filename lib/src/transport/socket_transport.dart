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

  /// How long a graceful [close] may wait for the peer before the socket is
  /// destroyed outright.
  static const Duration closeTimeout = Duration(seconds: 2);

  Socket? _socket;
  final StreamController<Uint8List> _incoming =
      StreamController<Uint8List>.broadcast(sync: true);
  bool _connected = false;
  bool _closed = false;
  bool _terminalEventSent = false;

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
    _terminalEventSent = false;
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

  void _onError(Object error, StackTrace stackTrace) {
    if (_closed || _terminalEventSent) {
      return;
    }
    _terminalEventSent = true;
    _connected = false;
    _incoming.addError(error, stackTrace);
  }

  void _onDone() {
    if (_closed || _terminalEventSent) {
      return;
    }
    _terminalEventSent = true;
    _connected = false;
    _incoming.addError(
      MqttTransportException('Connection closed by peer'),
      StackTrace.current,
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
        // close() only shuts down the write half and waits for the peer; a
        // half-open link or a NAT that never returns FIN would leave the
        // descriptor in CLOSE_WAIT indefinitely. Bound the graceful close,
        // then destroy unconditionally so the socket is always released.
        await socket.close().timeout(closeTimeout, onTimeout: () => socket);
      } on Object {
        // Best-effort close; destroy below is what actually frees the socket.
      }
      try {
        socket.destroy();
      } on Object {
        // Already gone.
      }
    }
    await _incoming.close();
  }
}
