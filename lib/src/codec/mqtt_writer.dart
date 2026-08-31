import 'dart:typed_data';

/// A builder for MQTT control packet byte buffers.
///
/// All multi-byte integers are written as unsigned big-endian values, as
/// required by the MQTT 5.0 specification.
final class MqttWriter {
  final BytesBuilder _builder = BytesBuilder();

  int get length => _builder.length;

  void writeByte(int value) {
    _builder.addByte(value & 0xFF);
  }

  /// Writes a Two Byte Integer.
  ///
  /// Out-of-range values are rejected rather than truncated: a silently
  /// wrapped length prefix produces a packet the peer cannot parse.
  void writeUint16(int value) {
    if (value < 0 || value > 0xFFFF) {
      throw ArgumentError.value(
        value,
        'value',
        'Must be between 0 and 65535 to fit a Two Byte Integer',
      );
    }
    _builder.addByte((value >> 8) & 0xFF);
    _builder.addByte(value & 0xFF);
  }

  /// Writes a Four Byte Integer, rejecting out-of-range values.
  void writeUint32(int value) {
    if (value < 0 || value > 0xFFFFFFFF) {
      throw ArgumentError.value(
        value,
        'value',
        'Must be between 0 and 4294967295 to fit a Four Byte Integer',
      );
    }
    _builder.addByte((value >> 24) & 0xFF);
    _builder.addByte((value >> 16) & 0xFF);
    _builder.addByte((value >> 8) & 0xFF);
    _builder.addByte(value & 0xFF);
  }

  void writeBytes(List<int> bytes) {
    _builder.add(bytes);
  }

  Uint8List toBytes() => _builder.takeBytes();
}
