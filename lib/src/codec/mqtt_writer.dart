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

  void writeUint16(int value) {
    _builder.addByte((value >> 8) & 0xFF);
    _builder.addByte(value & 0xFF);
  }

  void writeUint32(int value) {
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
