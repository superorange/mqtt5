import 'dart:io';

import 'socket_transport.dart';

/// A plain TCP transport backed by [Socket].
final class TcpTransport extends MqttSocketTransport {
  TcpTransport({
    required super.host,
    required super.port,
    super.timeout,
    super.sourceAddress,
  });

  @override
  Future<Socket> openSocket() {
    final source = sourceAddress;
    return Socket.connect(
      host,
      port,
      timeout: timeout,
      sourceAddress: source != null ? InternetAddress(source) : null,
    );
  }
}
