import 'dart:convert';
import 'dart:typed_data';

/// An independent, deliberately minimal MQTT 5 frame reader used to inspect
/// what actually crossed the wire. It shares no code with the library under
/// test, so a codec bug cannot hide itself by being "verified" with itself.

enum Dir { c2s, s2c }

const packetNames = {
  1: 'CONNECT',
  2: 'CONNACK',
  3: 'PUBLISH',
  4: 'PUBACK',
  5: 'PUBREC',
  6: 'PUBREL',
  7: 'PUBCOMP',
  8: 'SUBSCRIBE',
  9: 'SUBACK',
  10: 'UNSUBSCRIBE',
  11: 'UNSUBACK',
  12: 'PINGREQ',
  13: 'PINGRESP',
  14: 'DISCONNECT',
  15: 'AUTH',
};

const kConnect = 1,
    kConnack = 2,
    kPublish = 3,
    kPuback = 4,
    kPubrec = 5,
    kPubrel = 6,
    kPubcomp = 7,
    kSubscribe = 8,
    kSuback = 9,
    kUnsubscribe = 10,
    kUnsuback = 11,
    kPingreq = 12,
    kPingresp = 13,
    kDisconnect = 14,
    kAuth = 15;

final class WireFrame {
  WireFrame(this.dir, this.bytes, this.connection,
      {this.dropped = false, this.injected = false})
      : at = DateTime.now();

  /// True for bytes the fault proxy fabricated or rewrote.
  final bool injected;

  final Dir dir;
  final Uint8List bytes;
  final int connection;
  final DateTime at;
  bool dropped;

  int get type => bytes[0] >> 4;
  int get flags => bytes[0] & 0x0F;
  String get name => packetNames[type] ?? 'TYPE$type';

  int get _bodyStart {
    var i = 1;
    while (bytes[i] & 0x80 != 0) {
      i++;
    }
    return i + 1;
  }

  int get remainingLength => bytes.length - _bodyStart;

  /// The raw UTF-8 bytes of a PUBLISH Topic Name (no decoding, so nothing
  /// such as a BOM can be lost).
  Uint8List get publishTopicBytes {
    final o = _bodyStart;
    final n = (bytes[o] << 8) | bytes[o + 1];
    return Uint8List.sublistView(bytes, o + 2, o + 2 + n);
  }

  // ---- PUBLISH ----
  bool get dup => flags & 0x08 != 0;
  int get qos => (flags >> 1) & 0x03;
  bool get retain => flags & 0x01 != 0;

  ({
    String topic,
    int? packetId,
    List<(int, Object)> properties,
    Uint8List payload
  }) get publish {
    assert(type == kPublish);
    final r = _R(bytes, _bodyStart);
    final topic = r.str();
    final pid = qos > 0 ? r.u16() : null;
    final props = r.props();
    return (
      topic: topic,
      packetId: pid,
      properties: props,
      payload: Uint8List.sublistView(bytes, r.o),
    );
  }

  /// Packet identifier of PUBACK/PUBREC/PUBREL/PUBCOMP/SUBSCRIBE/SUBACK/...
  int get packetId {
    if (type == kPublish) return publish.packetId!;
    final r = _R(bytes, _bodyStart);
    return r.u16();
  }

  /// Reason code of an ack-like packet (null when omitted on the wire).
  int? get ackReasonCode {
    final start = _bodyStart;
    if (type == kDisconnect || type == kAuth) {
      return bytes.length > start ? bytes[start] : null;
    }
    if (type == kConnack) return bytes[start + 1];
    return bytes.length > start + 2 ? bytes[start + 2] : null;
  }

  List<(int, Object)> get ackProperties {
    final start = _bodyStart;
    final r = _R(bytes, start);
    switch (type) {
      case kDisconnect:
      case kAuth:
        if (bytes.length <= start + 1) return const [];
        r.o = start + 1;
        return r.props();
      case kConnack:
        r.o = start + 2;
        return r.props();
      case kPuback:
      case kPubrec:
      case kPubrel:
      case kPubcomp:
        if (bytes.length <= start + 3) return const [];
        r.o = start + 3;
        return r.props();
      case kSubscribe:
      case kSuback:
      case kUnsubscribe:
      case kUnsuback:
        r.o = start + 2;
        return r.props();
    }
    return const [];
  }

  // ---- CONNECT ----
  ({int flags, int keepAlive, List<(int, Object)> properties, String clientId})
      get connect {
    final r = _R(bytes, _bodyStart);
    r.str(); // "MQTT"
    r.u8(); // level
    final f = r.u8();
    final ka = r.u16();
    final props = r.props();
    final cid = r.str();
    return (flags: f, keepAlive: ka, properties: props, clientId: cid);
  }

  // ---- SUBSCRIBE ----
  List<(String, int)> get subscribeFilters {
    final r = _R(bytes, _bodyStart);
    r.u16();
    r.props();
    final out = <(String, int)>[];
    while (r.o < bytes.length) {
      out.add((r.str(), r.u8()));
    }
    return out;
  }

  @override
  String toString() {
    final b = StringBuffer('${dir.name} $name');
    if (type == kPublish) {
      final p = publish;
      b.write(' d${dup ? 1 : 0} q$qos r${retain ? 1 : 0} m${p.packetId} '
          "'${p.topic}' props=${p.properties} (${p.payload.length}B)");
    } else if (type >= kPuback && type <= kPubcomp) {
      b.write(' m$packetId rc=$ackReasonCode');
    } else if (type == kDisconnect || type == kAuth || type == kConnack) {
      b.write(' rc=$ackReasonCode props=$ackProperties');
    }
    if (dropped) b.write(' [DROPPED]');
    if (injected) b.write(' [INJECTED]');
    return b.toString();
  }
}

/// Splits a byte stream into frames using only the fixed header.
final class FrameSplitter {
  final _buf = BytesBuilder(copy: false);
  Uint8List _pending = Uint8List(0);

  List<Uint8List> add(List<int> chunk) {
    _buf.add(_pending);
    _buf.add(chunk);
    var data = _buf.takeBytes();
    final out = <Uint8List>[];
    var o = 0;
    while (true) {
      if (data.length - o < 2) break;
      var mult = 1, len = 0, i = 1;
      var complete = false;
      while (o + i < data.length && i <= 4) {
        final b = data[o + i];
        len += (b & 0x7F) * mult;
        mult *= 128;
        i++;
        if (b & 0x80 == 0) {
          complete = true;
          break;
        }
      }
      if (!complete) break;
      final total = i + len;
      if (data.length - o < total) break;
      out.add(Uint8List.fromList(data.sublist(o, o + total)));
      o += total;
    }
    _pending = Uint8List.fromList(data.sublist(o));
    return out;
  }
}

final class _R {
  _R(this.b, this.o);
  final Uint8List b;
  int o;

  int u8() => b[o++];
  int u16() {
    final v = (b[o] << 8) | b[o + 1];
    o += 2;
    return v;
  }

  int u32() {
    final v = (b[o] << 24) | (b[o + 1] << 16) | (b[o + 2] << 8) | b[o + 3];
    o += 4;
    return v;
  }

  int vbi() {
    var mult = 1, v = 0;
    while (true) {
      final x = b[o++];
      v += (x & 0x7F) * mult;
      mult *= 128;
      if (x & 0x80 == 0) return v;
    }
  }

  String str() {
    final n = u16();
    final s = utf8.decode(b.sublist(o, o + n));
    o += n;
    return s;
  }

  Uint8List bin() {
    final n = u16();
    final v = Uint8List.fromList(b.sublist(o, o + n));
    o += n;
    return v;
  }

  List<(int, Object)> props() {
    final len = vbi();
    final end = o + len;
    final out = <(int, Object)>[];
    while (o < end) {
      final id = vbi();
      switch (id) {
        case 0x01 || 0x17 || 0x19 || 0x24 || 0x25 || 0x28 || 0x29 || 0x2A:
          out.add((id, u8()));
        case 0x13 || 0x21 || 0x22 || 0x23:
          out.add((id, u16()));
        case 0x02 || 0x11 || 0x18 || 0x27:
          out.add((id, u32()));
        case 0x0B:
          out.add((id, vbi()));
        case 0x03 || 0x08 || 0x12 || 0x15 || 0x1A || 0x1C || 0x1F:
          out.add((id, str()));
        case 0x09 || 0x16:
          out.add((id, bin()));
        case 0x26:
          out.add((id, (str(), str())));
        default:
          throw FormatException('unknown property 0x${id.toRadixString(16)}');
      }
    }
    return out;
  }
}

Object? prop(List<(int, Object)> props, int id) {
  for (final (k, v) in props) {
    if (k == id) return v;
  }
  return null;
}

/// Hand-assembles a control packet: [first] is the fixed header byte, [body]
/// everything after the Remaining Length. Deliberately independent of the
/// library's encoder.
List<int> rawPacket(int first, List<int> body) {
  final out = <int>[first];
  var n = body.length;
  do {
    var b = n % 128;
    n ~/= 128;
    if (n > 0) b |= 0x80;
    out.add(b);
  } while (n > 0);
  return out..addAll(body);
}

List<int> rawString(String s) {
  final b = utf8.encode(s);
  return [b.length >> 8, b.length & 0xFF, ...b];
}

List<int> rawVbi(int n) {
  final out = <int>[];
  do {
    var b = n % 128;
    n ~/= 128;
    if (n > 0) b |= 0x80;
    out.add(b);
  } while (n > 0);
  return out;
}

/// A property section: VBI length followed by [props].
List<int> rawProps(List<int> props) => [...rawVbi(props.length), ...props];
