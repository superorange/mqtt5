import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:mqtt5/src/transport/tcp_transport.dart';
import 'package:test/test.dart';

void main() {
  test('TcpTransport round trips bytes with a local server', () async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final port = server.port;
    final serverReceived = <int>[];
    late Socket serverSide;

    server.listen((socket) {
      serverSide = socket;
      socket.listen((data) {
        serverReceived.addAll(data);
        socket.add(data);
      });
      socket.add(Uint8List.fromList([1, 2, 3]));
    });

    final transport = TcpTransport(
      host: '127.0.0.1',
      port: port,
      timeout: const Duration(seconds: 5),
    );
    final incoming = <Uint8List>[];
    final sub = transport.incoming.listen(incoming.add);

    await transport.connect();
    expect(transport.isConnected, isTrue);

    await _waitFor(() => incoming.isNotEmpty);
    expect(incoming.first, [1, 2, 3]);

    transport.add(Uint8List.fromList([9, 9]));
    await transport.flush();
    await _waitFor(() => serverReceived.isNotEmpty);
    expect(serverReceived, [9, 9]);

    await transport.close();
    expect(transport.isConnected, isFalse);
    await sub.cancel();
    await serverSide.close();
    await server.close();
  });

  test('TcpTransport connect fails on connection refused', () async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final port = server.port;
    await server.close();

    final transport = TcpTransport(
      host: '127.0.0.1',
      port: port,
      timeout: const Duration(seconds: 3),
    );
    await expectLater(
      transport.connect(),
      throwsA(isA<SocketException>()),
    );
  });

  test('TcpTransport cannot send while disconnected', () async {
    final transport = TcpTransport(host: '127.0.0.1', port: 1);
    expect(
      () => transport.add(Uint8List.fromList([1])),
      throwsA(isA<Exception>()),
    );
  });
}

Future<void> _waitFor(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('Timed out waiting for condition');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}
