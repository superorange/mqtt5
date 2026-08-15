import 'dart:async';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:mqtt5/testing.dart';
import 'package:test/test.dart';

void main() {
  group('malformed packets decode into MqttException', () {
    test('PUBLISH with DUP set on QoS 0', () {
      // 0x38 = PUBLISH | DUP, remaining length 6, topic "a/b", no properties.
      final bytes = Uint8List.fromList([
        0x38, 0x06, //
        0x00, 0x03, 0x61, 0x2f, 0x62,
        0x00,
      ]);
      expect(
        () => MqttPacketCodec.decode(bytes),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('PUBLISH with QoS 3', () {
      final bytes = Uint8List.fromList([
        0x36, 0x08, //
        0x00, 0x03, 0x61, 0x2f, 0x62,
        0x00, 0x01,
        0x00,
      ]);
      expect(
        () => MqttPacketCodec.decode(bytes),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('reserved QoS value', () {
      expect(
        () => MqttQos.fromValue(3),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('reserved Retain Handling value', () {
      expect(
        () => MqttSubscriptionOptions.fromByte(0x30),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('CONNECT with Will QoS 3', () {
      final writer = MqttPacketCodec.encode(
        MqttConnectPacket(
          clientId: 'c',
          will: MqttWill(
            topic: 'w',
            payload: Uint8List(0),
            qos: MqttQos.exactlyOnce,
          ),
        ),
      );
      // Force the Will QoS bits (4-3) to the reserved value 3.
      final connectFlagsIndex = 9;
      writer[connectFlagsIndex] |= 0x18;
      expect(
        () => MqttPacketCodec.decode(writer),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });
  });

  test('Maximum Packet Size accounts for the fixed header', () {
    final packet = MqttPacketCodec.encode(
      MqttPublishPacket(topicName: 'a/b', payload: Uint8List(10)),
    );
    // The packet is exactly `packet.length` bytes on the wire; a limit one
    // byte below that must reject it even though the Remaining Length alone
    // still fits.
    final decoder = MqttPacketDecoder(maximumPacketSize: packet.length - 1);
    expect(
      () => decoder.feed(packet),
      throwsA(isA<MqttPacketTooLargeException>()),
    );
    expect(
      MqttPacketDecoder(maximumPacketSize: packet.length).feed(packet),
      hasLength(1),
    );
  });

  test('a malformed packet from the broker is a protocol error, not a crash',
      () async {
    final transports = <MemoryTransport>[];
    final uncaught = <Object>[];
    late MqttClient client;

    await runZonedGuarded(() async {
      client = MqttClient(
        host: 'x',
        transportFactory: () {
          final transport = MemoryTransport();
          transports.add(transport);
          return transport;
        },
        reconnectManager: ReconnectManager(
          initialDelay: const Duration(milliseconds: 1),
        ),
      );
      final connecting = client.connect();
      await Future<void>.delayed(Duration.zero);
      transports.last.inject(
        MqttPacketCodec.encode(
          const MqttConnackPacket(sessionPresent: false),
        ),
      );
      await connecting;

      transports.last.inject(Uint8List.fromList([
        0x38, 0x06, //
        0x00, 0x03, 0x61, 0x2f, 0x62,
        0x00,
      ]));
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }, (error, stack) => uncaught.add(error));

    expect(uncaught, isEmpty, reason: 'must not escape as an unhandled error');
    expect(client.metrics.protocolErrorCount, 1);
    expect(transports, hasLength(2), reason: 'should have reconnected');
  });

  test('an exception thrown by a message listener does not kill the '
      'connection', () async {
    final transport = MemoryTransport();
    final uncaught = <Object>[];
    late MqttClient client;

    await runZonedGuarded(() async {
      client = MqttClient(host: 'x', transportFactory: () => transport);
      final connecting = client.connect();
      await Future<void>.delayed(Duration.zero);
      transport.inject(
        MqttPacketCodec.encode(const MqttConnackPacket(sessionPresent: false)),
      );
      await connecting;

      client.messages.listen((_) => throw StateError('listener blew up'));
      transport.inject(
        MqttPacketCodec.encode(
          MqttPublishPacket(topicName: 'a/b', payload: Uint8List(0)),
        ),
      );
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }, (error, stack) => uncaught.add(error));

    expect(uncaught, [isA<StateError>()]);
    expect(client.metrics.protocolErrorCount, 0);
    expect(client.state, MqttConnectionState.connected);
  });
}
