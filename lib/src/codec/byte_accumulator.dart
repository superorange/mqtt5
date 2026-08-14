import 'dart:typed_data';

/// A compacting byte buffer for assembling streamed chunks.
final class ByteAccumulator {
  ByteAccumulator({int initialCapacity = 1024})
      : _data = Uint8List(initialCapacity < 16 ? 16 : initialCapacity);

  Uint8List _data;
  int _start = 0;
  int _end = 0;

  int get available => _end - _start;

  bool get isEmpty => available == 0;

  void append(Uint8List chunk) {
    if (chunk.isEmpty) {
      return;
    }
    if (_start == _end) {
      _start = 0;
      _end = 0;
    } else if (_start > 0 && _start * 2 > _end) {
      final remaining = available;
      _data.setRange(0, remaining, _data, _start);
      _start = 0;
      _end = remaining;
    }
    _ensureCapacity(chunk.length);
    _data.setRange(_end, _end + chunk.length, chunk);
    _end += chunk.length;
  }

  int peekByte(int index) => _data[_start + index];

  /// Removes and returns the first [count] bytes as a view.
  Uint8List take(int count) {
    final result = Uint8List.sublistView(_data, _start, _start + count);
    _start += count;
    return result;
  }

  void _ensureCapacity(int additional) {
    final required = _end + additional;
    if (required <= _data.length) {
      return;
    }
    var capacity = _data.length;
    while (capacity < required) {
      capacity *= 2;
    }
    final grown = Uint8List(capacity);
    grown.setRange(0, _end, _data);
    _data = grown;
  }
}
