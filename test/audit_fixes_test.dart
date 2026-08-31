/// Regression tests for the audit findings fixed after the P1 round.
///
/// Each group names the finding it locks in; see the audit register for the
/// reproduction that motivated it.
library;

import 'dart:typed_data';

import 'package:mqtt5/src/client/mqtt_client.dart';
import 'package:mqtt5/src/codec/mqtt_reader.dart';
import 'package:mqtt5/src/codec/mqtt_writer.dart';
import 'package:mqtt5/src/codec/variable_byte_integer.dart';
import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:mqtt5/src/mqtt_qos.dart';
import 'package:mqtt5/src/packet/connack.dart';
import 'package:mqtt5/src/packet/connect.dart';
import 'package:mqtt5/src/packet/mqtt_packet.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/packet/publish.dart';
import 'package:mqtt5/src/packet/suback.dart';
import 'package:mqtt5/src/packet/subscribe.dart';
import 'package:mqtt5/src/property/mqtt_property.dart';
import 'package:mqtt5/src/property/property_codec.dart';
import 'package:mqtt5/src/subscription.dart';
import 'package:mqtt5/src/transport/memory_transport.dart';
import 'package:test/test.dart';

void main() {
  group('P2-07 integer widths are range checked, never truncated', () {
    test('writeUint16 rejects values above 65535', () {
      expect(() => MqttWriter().writeUint16(0x10000), throwsArgumentError);
      expect(() => MqttWriter().writeUint16(-1), throwsArgumentError);
    });

    test('writeUint32 rejects values above 4294967295', () {
      expect(() => MqttWriter().writeUint32(0x100000000), throwsArgumentError);
    });

    test('an oversized will payload is rejected, not silently truncated', () {
      expect(
        () => MqttConnectPacket(
          clientId: 'c',
          will: MqttWill(topic: 'w', payload: Uint8List(70000)),
        ),
        throwsArgumentError,
      );
    });

    test('an oversized password is rejected', () {
      expect(
        () => MqttConnectPacket(clientId: 'c', password: Uint8List(70000)),
        throwsArgumentError,
      );
    });

    test('a will payload at the limit still round-trips', () {
      final packet = MqttConnectPacket(
        clientId: 'c',
        will: MqttWill(topic: 'w', payload: Uint8List(0xFFFF)),
      );
      final decoded =
          MqttPacketCodec.decode(MqttPacketCodec.encode(packet))
              as MqttConnectPacket;
      expect(decoded.will!.payload, hasLength(0xFFFF));
    });
  });

  group('P3-02 Variable Byte Integers must be minimally encoded', () {
    test('0x80 0x00 is malformed', () {
      expect(
        () => VariableByteInteger.decode(
            MqttReader(Uint8List.fromList([0x80, 0x00]))),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('0x81 0x00 is malformed', () {
      expect(
        () => VariableByteInteger.decode(
            MqttReader(Uint8List.fromList([0x81, 0x00]))),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('a single zero byte is still valid', () {
      expect(
        VariableByteInteger.decode(MqttReader(Uint8List.fromList([0x00]))),
        0,
      );
    });

    test('genuine multi-byte encodings still decode', () {
      for (final value in [128, 16383, 16384, 2097151, 268435455]) {
        final bytes = VariableByteInteger.encode(value);
        expect(VariableByteInteger.decode(MqttReader(bytes)), value,
            reason: 'round trip for $value');
      }
    });
  });

  group('P3-01 packet identifier 0 is rejected on decode', () {
    test('SUBACK with identifier 0', () {
      expect(
        () => MqttPacketCodec.decode(
            Uint8List.fromList([0x90, 0x04, 0x00, 0x00, 0x00, 0x00])),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('PUBACK with identifier 0', () {
      expect(
        () => MqttPacketCodec.decode(
            Uint8List.fromList([0x40, 0x02, 0x00, 0x00])),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });
  });

  group('P3-03 Subscription Identifier repeats only in PUBLISH', () {
    test('twice in a PUBLISH is accepted', () {
      final writer = MqttWriter();
      PropertyCodec.encode(
        writer,
        const [SubscriptionIdentifier(1), SubscriptionIdentifier(2)],
        MqttPropertyContext.publish,
      );
      final decoded = PropertyCodec.decode(
          MqttReader(writer.toBytes()), MqttPropertyContext.publish);
      expect(decoded, hasLength(2));
    });

    test('twice in a SUBSCRIBE is a protocol error', () {
      expect(
        () => PropertyCodec.encode(
          MqttWriter(),
          const [SubscriptionIdentifier(1), SubscriptionIdentifier(2)],
          MqttPropertyContext.subscribe,
        ),
        throwsA(isA<MqttProtocolException>()),
      );
    });
  });

  group('client behaviour', () {
    test('P2-04 a broker exceeding our Receive Maximum is disconnected',
        () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        transportFactory: () => transport,
        autoReconnect: false,
      );
      client.errors.listen((_) {});
      client.messages.listen((_) {});
      await _handshake(client, transport, receiveMaximum: 2);

      // Three concurrent QoS2 exchanges, none of them completed by a PUBREL.
      for (var id = 1; id <= 3; id++) {
        transport.inject(MqttPacketCodec.encode(MqttPublishPacket(
          topicName: 't',
          payload: Uint8List(0),
          qos: MqttQos.exactlyOnce,
          packetIdentifier: id,
        )));
      }
      await Future<void>.delayed(const Duration(milliseconds: 60));
      expect(client.state.name, 'disconnected',
          reason: 'exceeding Receive Maximum must end the connection');
    });

    test('P2-06 the subscription identifier survives a session loss', () async {
      var transport = MemoryTransport();
      final client = MqttClient(host: 'h', transportFactory: () => transport);
      await _handshake(client, transport);

      final sub = client.subscribe('sensors/#', subscriptionIdentifier: 7);
      final s = await _next(transport) as MqttSubscribePacket;
      transport.inject(MqttPacketCodec.encode(MqttSubackPacket(
          packetIdentifier: s.packetIdentifier, reasonCodes: const [0])));
      await sub;

      final old = transport;
      transport = MemoryTransport();
      old.injectError(Exception('down'));
      await _waitFor(() => transport.outgoing.isNotEmpty);
      transport.takeOutgoing();
      transport.inject(MqttPacketCodec.encode(
          const MqttConnackPacket(sessionPresent: false)));

      final again = await _next(transport) as MqttSubscribePacket;
      expect(again.properties, contains(const SubscriptionIdentifier(7)));
      expect(again.subscriptions.single.topicFilter, 'sensors/#');
    });

    test('P2-13 Response Information reaches the application', () async {
      final transport = MemoryTransport();
      final client = MqttClient(host: 'h', transportFactory: () => transport);
      await _handshake(
        client,
        transport,
        extraProperties: const [ResponseInformation('reply/abc')],
      );
      expect(client.responseInformation, 'reply/abc');
      expect(client.connackProperties,
          contains(const ResponseInformation('reply/abc')));
    });

    test('P2-14 a subscription identifier is refused when unsupported',
        () async {
      final transport = MemoryTransport();
      final client = MqttClient(host: 'h', transportFactory: () => transport);
      await _handshake(
        client,
        transport,
        extraProperties: const [SubscriptionIdentifierAvailable(0)],
      );
      expect(
        () => client.subscribe('a/b', subscriptionIdentifier: 1),
        throwsA(isA<MqttFlowControlException>()),
      );
    });

    test('P3-09 No Local on a shared subscription is refused', () async {
      final transport = MemoryTransport();
      final client = MqttClient(host: 'h', transportFactory: () => transport);
      await _handshake(client, transport);
      expect(
        () => client.subscribe(
          r'$share/g/a/b',
          options: const MqttSubscriptionOptions(noLocal: true),
        ),
        throwsArgumentError,
      );
    });

    test('P2-05 operationTimeout bounds the whole publish, not just the ack',
        () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        transportFactory: () => transport,
        operationTimeout: const Duration(milliseconds: 120),
      );
      await _handshake(client, transport, brokerReceiveMaximum: 1);

      // One slot; the broker never acknowledges anything.
      var settled = 0;
      for (var i = 0; i < 20; i++) {
        client.publish('t', Uint8List(1), qos: MqttQos.atLeastOnce).then(
              (_) => settled++,
              onError: (Object _) => settled++,
            );
      }
      await Future<void>.delayed(const Duration(seconds: 1));
      expect(settled, 20,
          reason: 'every publish must settle within operationTimeout, '
              'including the ones queued on flow control');
    });

    test('R2 disconnect reclaims publications abandoned by a timeout',
        () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        transportFactory: () => transport,
        operationTimeout: const Duration(milliseconds: 40),
      );
      await _handshake(client, transport);

      for (var i = 0; i < 5; i++) {
        try {
          await client.publish('t', Uint8List(1), qos: MqttQos.atLeastOnce);
        } on MqttTimeoutException {
          // expected
        }
      }
      // Kept while the session is live, so a resume can retransmit them.
      expect(client.inflightCount, 5);

      await client.disconnect();
      expect(client.inflightCount, 0,
          reason: 'an explicit teardown must release the session slots');
    });

    test('connect() validates arguments even when already connected', () async {
      final transport = MemoryTransport();
      final client = MqttClient(host: 'h', transportFactory: () => transport);
      await _handshake(client, transport);
      await expectLater(
        client.connect(keepAlive: const Duration(days: 100)),
        throwsArgumentError,
      );
    });

    test('connackTimeout must be positive', () async {
      final transport = MemoryTransport();
      final client = MqttClient(host: 'h', transportFactory: () => transport);
      await expectLater(
        client.connect(connackTimeout: Duration.zero),
        throwsArgumentError,
      );
    });
  });
}

Future<void> _handshake(
  MqttClient client,
  MemoryTransport transport, {
  int receiveMaximum = 65535,
  int? brokerReceiveMaximum,
  List<MqttProperty> extraProperties = const [],
}) async {
  final future = client.connect(receiveMaximum: receiveMaximum);
  await _next(transport);
  transport.inject(MqttPacketCodec.encode(MqttConnackPacket(
    sessionPresent: false,
    properties: [
      if (brokerReceiveMaximum != null) ReceiveMaximum(brokerReceiveMaximum),
      ...extraProperties,
    ],
  )));
  await future;
}

Future<void> _waitFor(bool Function() predicate) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!predicate()) {
    if (DateTime.now().isAfter(deadline)) fail('timed out');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

Future<MqttPacket> _next(MemoryTransport transport) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (true) {
    final bytes = transport.takeOutgoingBytes();
    if (bytes.isNotEmpty) return MqttPacketCodec.decode(bytes);
    if (DateTime.now().isAfter(deadline)) {
      fail('Timed out waiting for a packet from the client');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
