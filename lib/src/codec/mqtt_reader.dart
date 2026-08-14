import 'dart:typed_data';

import '../exception/mqtt_exception.dart';

/// A cursor over a single MQTT control packet byte buffer.
///
/// All multi-byte integers are read as unsigned big-endian values, as
/// required by the MQTT 5.0 specification.
final class MqttReader {
  MqttReader(Uint8List data)
      : _data = data,
        _view = ByteData.sublistView(data);

  final Uint8List _data;
  final ByteData _view;
  int _offset = 0;

  int get offset => _offset;

  int get length => _data.length;

  int get remainingLength => _data.length - _offset;

  bool get hasRemaining => _offset < _data.length;

  int readByte() {
    _ensureAvailable(1);
    return _data[_offset++];
  }

  int readUint16() {
    _ensureAvailable(2);
    final value = _view.getUint16(_offset);
    _offset += 2;
    return value;
  }

  int readUint32() {
    _ensureAvailable(4);
    final value = _view.getUint32(_offset);
    _offset += 4;
    return value;
  }

  Uint8List readBytes(int length) {
    if (length < 0) {
      throw MqttMalformedPacketException('Negative byte length: $length');
    }
    _ensureAvailable(length);
    final result = Uint8List.sublistView(_data, _offset, _offset + length);
    _offset += length;
    return result;
  }

  Uint8List readRemaining() => readBytes(remainingLength);

  /// Skips [length] bytes without copying them.
  void skip(int length) {
    if (length < 0) {
      throw MqttMalformedPacketException('Negative skip length: $length');
    }
    _ensureAvailable(length);
    _offset += length;
  }

  void _ensureAvailable(int count) {
    if (remainingLength < count) {
      throw MqttIncompletePacketException(
        'Not enough bytes: need $count, have $remainingLength',
      );
    }
  }
}
