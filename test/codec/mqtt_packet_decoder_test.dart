import 'dart:math';
import 'dart:typed_data';

import 'package:mqtt5/src/codec/mqtt_packet_decoder.dart';
import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:mqtt5/src/packet/connack.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/packet/mqtt_reason_code.dart';
import 'package:mqtt5/src/packet/publish.dart';
import 'package:mqtt5/src/mqtt_qos.dart';
import 'package:test/test.dart';

void main() {
  final connackBytes = MqttPacketCodec.encode(const MqttConnackPacket(
    sessionPresent: false,
    reasonCode: MqttReasonCode.success,
  ));

  final publishBytes = MqttPacketCodec.encode(MqttPublishPacket(
    topicName: 'some/topic',
    payload: Uint8List.fromList(List.generate(50, (i) => i)),
    qos: MqttQos.atLeastOnce,
    packetIdentifier: 7,
  ));

  group('fragmentation', () {
    test('one byte at a time', () {
      final decoder = MqttPacketDecoder();
      final packets = <dynamic>[];
      for (final byte in connackBytes) {
        packets.addAll(decoder.feed(Uint8List.fromList([byte])));
      }
      expect(packets, hasLength(1));
      expect(packets.single, isA<MqttConnackPacket>());
      expect(decoder.bufferedBytes, 0);
    });

    test('two bytes at a time', () {
      final decoder = MqttPacketDecoder();
      final packets = <dynamic>[];
      for (var i = 0; i < publishBytes.length; i += 2) {
        final end = min(i + 2, publishBytes.length);
        packets.addAll(decoder.feed(Uint8List.sublistView(publishBytes, i, end)));
      }
      expect(packets, hasLength(1));
      expect(packets.single, isA<MqttPublishPacket>());
    });

    test('packet A + packet B in one chunk', () {
      final decoder = MqttPacketDecoder();
      final combined = Uint8List.fromList([
        ...connackBytes,
        ...publishBytes,
      ]);
      final packets = decoder.feed(combined);
      expect(packets, hasLength(2));
      expect(packets[0], isA<MqttConnackPacket>());
      expect(packets[1], isA<MqttPublishPacket>());
      expect(decoder.bufferedBytes, 0);
    });

    test('packet A + packet B + half packet C in one chunk', () {
      final decoder = MqttPacketDecoder();
      final combined = Uint8List.fromList([
        ...connackBytes,
        ...publishBytes,
        ...publishBytes.sublist(0, 3),
      ]);
      final packets = decoder.feed(combined);
      expect(packets, hasLength(2));
      expect(decoder.bufferedBytes, 3);
      // Feed the rest of C.
      final rest = decoder.feed(
        Uint8List.fromList(publishBytes.sublist(3)),
      );
      expect(rest, hasLength(1));
      expect(rest.single, isA<MqttPublishPacket>());
    });

    test('remaining length spans chunks', () {
      // A publish whose remaining length (VBI) is at least 2 bytes: use a
      // large payload to force a multi-byte VBI.
      final bigPublish = MqttPacketCodec.encode(MqttPublishPacket(
        topicName: 'big',
        payload: Uint8List.fromList(List.filled(200, 0x42)),
        qos: MqttQos.atLeastOnce,
        packetIdentifier: 1,
      ));
      expect(bigPublish[1] & 0x80, 0x80, reason: 'VBI is multi-byte');

      final decoder = MqttPacketDecoder();
      // Feed the fixed header byte and the first VBI byte only.
      final packets = decoder.feed(Uint8List.fromList(bigPublish.sublist(0, 2)));
      expect(packets, isEmpty);
      expect(decoder.bufferedBytes, 2);
      final rest = decoder.feed(
        Uint8List.fromList(bigPublish.sublist(2)),
      );
      expect(rest, hasLength(1));
      expect(MqttPacketCodec.encode(rest.single), bigPublish);
    });

    test('100 packets merged into one chunk', () {
      final decoder = MqttPacketDecoder();
      final stream = <int>[];
      for (var i = 0; i < 100; i++) {
        stream.addAll(connackBytes);
      }
      final packets = decoder.feed(Uint8List.fromList(stream));
      expect(packets, hasLength(100));
      for (final packet in packets) {
        expect(packet, isA<MqttConnackPacket>());
      }
    });

    test('random chunk sizes reassemble correctly', () {
      final random = Random(12345);
      final sources = <Uint8List>[
        connackBytes,
        publishBytes,
        MqttPacketCodec.encode(MqttPublishPacket(
          topicName: 't/2',
          payload: Uint8List.fromList([9, 8, 7]),
        )),
        connackBytes,
      ];
      final stream = Uint8List.fromList([
        for (final s in sources) ...s,
      ]);

      final decoder = MqttPacketDecoder();
      final decoded = <dynamic>[];
      var offset = 0;
      while (offset < stream.length) {
        final chunkSize = 1 + random.nextInt(9);
        final end = min(offset + chunkSize, stream.length);
        decoded.addAll(
          decoder.feed(Uint8List.sublistView(stream, offset, end)),
        );
        offset = end;
      }
      expect(decoded, hasLength(4));
      expect(MqttPacketCodec.encode(decoded[0]), sources[0]);
      expect(MqttPacketCodec.encode(decoded[1]), sources[1]);
      expect(MqttPacketCodec.encode(decoded[2]), sources[2]);
      expect(MqttPacketCodec.encode(decoded[3]), sources[3]);
    });
  });

  group('payload isolation', () {
    test('decoded payloads do not alias the shared buffer', () {
      final decoder = MqttPacketDecoder();
      final p1 = MqttPacketCodec.encode(MqttPublishPacket(
        topicName: 'a',
        payload: Uint8List.fromList([0x11, 0x11]),
      ));
      final p2 = MqttPacketCodec.encode(MqttPublishPacket(
        topicName: 'b',
        payload: Uint8List.fromList([0x22, 0x22]),
      ));
      final first = decoder.feed(p1);
      final second = decoder.feed(p2);
      expect(first, hasLength(1));
      expect(second, hasLength(1));
      expect((first[0] as MqttPublishPacket).payload, [0x11, 0x11]);
      expect((second[0] as MqttPublishPacket).payload, [0x22, 0x22]);
    });
  });

  group('guards', () {
    test('declared size exceeding maximum throws too large', () {
      final decoder = MqttPacketDecoder(maximumPacketSize: 100);
      // CONNACK declaring remaining length 101 (VBI: 0xE5 0x00)
      expect(
        () => decoder.feed(Uint8List.fromList([0x20, 0xE5, 0x00])),
        throwsA(isA<MqttPacketTooLargeException>()),
      );
    });

    test('VBI longer than 4 bytes is malformed', () {
      final decoder = MqttPacketDecoder();
      expect(
        () => decoder.feed(
          Uint8List.fromList([0x20, 0x80, 0x80, 0x80, 0x80, 0x01]),
        ),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });
  });

  group('fuzz', () {
    test('random bytes never crash or hang', () {
      final random = Random(999);
      final decoder = MqttPacketDecoder();
      for (var round = 0; round < 500; round++) {
        final length = random.nextInt(64);
        final chunk = Uint8List.fromList(
          List.generate(length, (_) => random.nextInt(256)),
        );
        try {
          final packets = decoder.feed(chunk);
          // Any packet produced must itself round-trip.
          for (final packet in packets) {
            expect(
              MqttPacketCodec.decode(MqttPacketCodec.encode(packet)),
              isNotNull,
            );
          }
        } on MqttException {
          // Protocol errors are the only acceptable failure mode.
        }
      }
    });

    test('corrupted remaining length never hangs', () {
      final decoder = MqttPacketDecoder();
      expect(
        () => decoder.feed(
          Uint8List.fromList([0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
        ),
        throwsA(isA<MqttException>()),
      );
    });

    test('illegal flags produce protocol error', () {
      final decoder = MqttPacketDecoder();
      // PUBREL (type 6) with flags 0x0 instead of 0x2.
      expect(
        () => decoder.feed(Uint8List.fromList([0x60, 0x02, 0x00, 0x01])),
        throwsA(isA<MqttProtocolException>()),
      );
    });

    test('truncated packet waits for more data without error', () {
      final decoder = MqttPacketDecoder();
      expect(decoder.feed(Uint8List.fromList([0x30, 0x05, 0x00])), isEmpty);
      expect(decoder.bufferedBytes, 3);
    });
  });
}
