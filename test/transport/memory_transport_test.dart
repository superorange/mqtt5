import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:mqtt5/testing.dart';
import 'package:test/test.dart';

void main() {
  test('injectError marks the transport disconnected', () async {
    final transport = MemoryTransport();
    await transport.connect();
    expect(transport.isConnected, isTrue);

    Object? seen;
    final sub = transport.incoming.listen((_) {}, onError: (Object e) {
      seen = e;
    });
    transport.injectError(MqttTransportException('boom'));

    expect(transport.isConnected, isFalse);
    expect(seen, isA<MqttTransportException>());
    expect(
      () => transport.add(Uint8List(1)),
      throwsA(isA<MqttTransportException>()),
    );
    await sub.cancel();
    await transport.close();
  });

  test('add after close throws', () async {
    final transport = MemoryTransport();
    await transport.connect();
    await transport.close();
    expect(
      () => transport.add(Uint8List(1)),
      throwsA(isA<MqttTransportException>()),
    );
  });

  test('injectDone reports a peer close as an error', () async {
    final transport = MemoryTransport();
    await transport.connect();
    Object? seen;
    final sub = transport.incoming.listen((_) {}, onError: (Object e) {
      seen = e;
    });
    transport.injectDone();
    expect(transport.isConnected, isFalse);
    expect(seen, isA<MqttTransportException>());
    await sub.cancel();
    await transport.close();
  });
}
