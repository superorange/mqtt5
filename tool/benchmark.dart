// Micro-benchmarks for the codec.
//
// Usage: dart run tool/benchmark.dart
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:mqtt5/src/codec/variable_byte_integer.dart';

void main() {
  _bench('VariableByteInteger encode', () {
    VariableByteInteger.encode(268435455);
  });

  final publish = MqttPublishPacket(
    topicName: 'bench/topic',
    payload: Uint8List.fromList(List.filled(1024, 0x42)),
    qos: MqttQos.atLeastOnce,
    packetIdentifier: 7,
    properties: const [
      PayloadFormatIndicator(1),
      MessageExpiryInterval(60),
      ContentType('application/octet-stream'),
      UserProperty('k', 'v'),
    ],
  );
  final encoded = MqttPacketCodec.encode(publish);
  _bench('PUBLISH encode (1 KB)', () {
    MqttPacketCodec.encode(publish);
  });
  _bench('PUBLISH decode (1 KB)', () {
    MqttPacketCodec.decode(encoded);
  });

  for (final size in [10, 1024, 10 * 1024, 100 * 1024]) {
    final packet = MqttPublishPacket(
      topicName: 'bench/topic',
      payload: Uint8List.fromList(List.filled(size, 0x01)),
      qos: MqttQos.atMostOnce,
    );
    final bytes = MqttPacketCodec.encode(packet);
    _bench('PUBLISH encode ${_size(size)}', () {
      MqttPacketCodec.encode(packet);
    });
    _bench('PUBLISH decode ${_size(size)}', () {
      MqttPacketCodec.decode(bytes);
    });
  }
}

String _size(int bytes) {
  if (bytes < 1024) {
    return '$bytes B';
  }
  return '${bytes ~/ 1024} KB';
}

void _bench(String name, void Function() body) {
  // Warm up.
  for (var i = 0; i < 1000; i++) {
    body();
  }
  const iterations = 100000;
  final stopwatch = Stopwatch()..start();
  for (var i = 0; i < iterations; i++) {
    body();
  }
  stopwatch.stop();
  final usPerOp = stopwatch.elapsedMicroseconds / iterations;
  final opsPerSec = iterations / stopwatch.elapsedMicroseconds * 1e6;
  print('${name.padRight(30)} '
      '${usPerOp.toStringAsFixed(2).padLeft(8)} us/op  '
      '${opsPerSec.toStringAsFixed(0).padLeft(10)} ops/s');
}
