import 'dart:typed_data';

import '../exception/mqtt_exception.dart';
import '../packet/mqtt_packet.dart';
import '../packet/mqtt_packet_codec.dart';
import 'byte_accumulator.dart';

/// An incremental MQTT packet decoder.
///
/// TCP has no packet boundaries, so this decoder accumulates streamed chunks
/// and emits complete [MqttPacket] objects as soon as enough bytes are
/// buffered. It handles packets split across chunk boundaries, multiple
/// packets in a single chunk, and Remaining Length fields that span chunks.
final class MqttPacketDecoder {
  MqttPacketDecoder({
    this.maximumPacketSize = variableByteIntegerMax,
  });

  static const int variableByteIntegerMax = 268435455;

  /// The maximum accepted packet size (fixed header + body).
  final int maximumPacketSize;

  final ByteAccumulator _accumulator = ByteAccumulator();

  int get bufferedBytes => _accumulator.available;

  /// Feeds [chunk] and returns every complete packet extracted.
  ///
  /// Throws [MqttMalformedPacketException] or [MqttProtocolException] for
  /// malformed input and [MqttPacketTooLargeException] when a declared
  /// Remaining Length exceeds [maximumPacketSize].
  List<MqttPacket> feed(Uint8List chunk) {
    _accumulator.append(chunk);
    final packets = <MqttPacket>[];
    while (true) {
      final length = _peekPacketLength();
      if (length == null) {
        break;
      }
      final bytes = _accumulator.take(length);
      packets.add(MqttPacketCodec.decode(bytes));
    }
    return packets;
  }

  /// Returns the total length in bytes of the next complete packet, or null
  /// if more data is required.
  int? _peekPacketLength() {
    if (_accumulator.available < 2) {
      return null;
    }
    var value = 0;
    var multiplier = 1;
    for (var i = 0; i < 4; i++) {
      if (_accumulator.available < i + 2) {
        return null;
      }
      final b = _accumulator.peekByte(1 + i);
      value += (b & 0x7F) * multiplier;
      if ((b & 0x80) == 0) {
        // Maximum Packet Size covers the whole packet, fixed header included
        // (specification section 3.1.2.11.4).
        final total = 1 + i + 1 + value;
        if (total > maximumPacketSize) {
          throw MqttPacketTooLargeException(
            'Packet size $total exceeds maximum $maximumPacketSize',
          );
        }
        if (_accumulator.available < total) {
          return null;
        }
        return total;
      }
      multiplier <<= 7;
    }
    throw MqttMalformedPacketException(
      'Variable Byte Integer exceeds 4 bytes',
    );
  }
}
