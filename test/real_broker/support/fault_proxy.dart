import 'dart:async';
import 'dart:collection';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'wire.dart';

/// Network conditions applied to one direction of every relayed connection.
final class LinkConditions {
  /// One-way delay added to every segment.
  Duration latency = Duration.zero;

  /// Uniform random extra delay in [0, jitter]. Order is always preserved, as
  /// on a real TCP stream.
  Duration jitter = Duration.zero;

  /// Throughput cap; null means unlimited.
  int? bytesPerSecond;

  /// Splits every write into segments of at most this many bytes (each its
  /// own socket write), so the receiver sees packets arrive in pieces.
  int? maxSegment;

  /// Swallows everything in this direction while keeping both sockets open:
  /// a half-dead path.
  bool blackhole = false;

  /// Holds everything back (in order) until set to false again: a stalled
  /// path where TCP keeps the bytes and delivers them late.
  bool paused = false;

  bool get shaping =>
      paused ||
      latency > Duration.zero ||
      jitter > Duration.zero ||
      bytesPerSecond != null ||
      maxSegment != null;

  void reset() {
    latency = Duration.zero;
    jitter = Duration.zero;
    bytesPerSecond = null;
    maxSegment = null;
    blackhole = false;
    paused = false;
  }
}

/// A TCP relay placed between the client and a real broker to inject network
/// faults: dropping individual packets, silently black-holing a link, cutting
/// connections, and shaping traffic (latency, jitter, bandwidth, segmentation)
/// to emulate weak networks. Both ends are real; only the network misbehaves.
final class FaultProxy {
  FaultProxy._(this._server, this.upstreamPort);

  final ServerSocket _server;
  final int upstreamPort;
  final List<_Link> _links = [];
  final List<WireFrame> frames = [];
  final _frameEvents = StreamController<WireFrame>.broadcast(sync: true);
  final _random = Random(42);
  int _connections = 0;
  Timer? _chaos;
  int chaosCuts = 0;

  /// Whether frames are kept in [frames] (turn off for long soak runs).
  bool record = true;

  /// Return true to swallow a frame instead of forwarding it.
  bool Function(WireFrame frame)? dropWhen;

  /// While true nothing is forwarded in either direction, but both sockets
  /// stay open: a dead link that TCP has not noticed yet.
  bool blackhole = false;

  /// While true new client connections are closed immediately.
  bool refuse = false;

  /// Return replacement bytes for a frame (an empty list drops it, null
  /// forwards it unchanged). Used to corrupt what a real broker sent.
  List<Uint8List>? Function(WireFrame frame)? rewrite;

  /// Conditions for client -> broker and broker -> client traffic.
  final LinkConditions c2s = LinkConditions();
  final LinkConditions s2c = LinkConditions();

  int get port => _server.port;
  int get connections => _connections;
  Stream<WireFrame> get onFrame => _frameEvents.stream;

  static Future<FaultProxy> start(int upstreamPort) async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final proxy = FaultProxy._(server, upstreamPort);
    server.listen(proxy._accept);
    return proxy;
  }

  Iterable<WireFrame> sent(int type) =>
      frames.where((f) => f.dir == Dir.c2s && f.type == type);
  Iterable<WireFrame> received(int type) =>
      frames.where((f) => f.dir == Dir.s2c && f.type == type);

  Future<void> _accept(Socket client) async {
    if (refuse) {
      client.destroy();
      return;
    }
    final id = ++_connections;
    Socket upstream;
    try {
      upstream = await Socket.connect('127.0.0.1', upstreamPort);
    } on SocketException {
      client.destroy();
      return;
    }
    // Write failures surface on `done`; without a handler they are uncaught.
    client.done.then((_) {}, onError: (_) {});
    upstream.done.then((_) {}, onError: (_) {});
    final link = _Link(client, upstream, this);
    _links.add(link);
    final c2sSplit = FrameSplitter();
    final s2cSplit = FrameSplitter();
    client.listen(
      (data) => _relay(link, Dir.c2s, c2sSplit.add(data), id),
      onError: (_) => link.destroy(),
      onDone: () => link.destroy(),
    );
    // While black-holed, the broker giving up on the link must not reach the
    // client either: on a real dead path its FIN/RST would be lost too.
    upstream.listen(
      (data) => _relay(link, Dir.s2c, s2cSplit.add(data), id),
      onError: (_) => _s2cDead ? link.destroyUpstream() : link.destroy(),
      onDone: () => _s2cDead ? link.destroyUpstream() : link.destroy(),
    );
  }

  bool get _s2cDead => blackhole || s2c.blackhole;

  void _relay(_Link link, Dir dir, List<Uint8List> chunks, int id) {
    final cond = dir == Dir.c2s ? c2s : s2c;
    for (final bytes in chunks) {
      final frame = WireFrame(dir, bytes, id);
      final drop =
          blackhole || cond.blackhole || (dropWhen?.call(frame) ?? false);
      final replacement = drop ? null : rewrite?.call(frame);
      frame.dropped = drop || replacement != null;
      if (record) frames.add(frame);
      if (!_frameEvents.isClosed) _frameEvents.add(frame);
      if (drop || link.dead) continue;
      for (final out in replacement ?? [bytes]) {
        if (replacement != null) {
          final injected = WireFrame(dir, out, id, injected: true);
          if (record) frames.add(injected);
          if (!_frameEvents.isClosed) _frameEvents.add(injected);
        }
        link.pipe(dir).send(out, cond);
      }
    }
  }

  /// Waits for the next frame matching [test] (searching history first when
  /// [includeHistory] is set).
  Future<WireFrame> next(bool Function(WireFrame f) test,
      {Duration timeout = const Duration(seconds: 10),
      bool includeHistory = false}) {
    if (includeHistory) {
      for (final f in frames) {
        if (test(f)) return Future.value(f);
      }
    }
    return onFrame.firstWhere(test).timeout(timeout);
  }

  /// Writes raw bytes to the client on the most recent live connection, as if
  /// the broker had sent them.
  void injectToClient(List<int> raw) {
    final link = _links.lastWhere((l) => !l.dead);
    final data = Uint8List.fromList(raw);
    if (record) {
      frames.add(WireFrame(Dir.s2c, data, _connections, injected: true));
    }
    link.client.add(data);
  }

  /// Resets every relayed connection (both directions).
  void cutAll() {
    for (final link in _links) {
      link.destroy();
    }
    _links.clear();
  }

  /// Cuts all connections at random intervals in [minEvery, maxEvery].
  void startChaos(Duration minEvery, Duration maxEvery) {
    stopChaos();
    void schedule() {
      final span = maxEvery.inMilliseconds - minEvery.inMilliseconds;
      final wait = minEvery.inMilliseconds + _random.nextInt(span + 1);
      _chaos = Timer(Duration(milliseconds: wait), () {
        if (_links.any((l) => !l.dead)) chaosCuts++;
        cutAll();
        schedule();
      });
    }

    schedule();
  }

  void stopChaos() {
    _chaos?.cancel();
    _chaos = null;
  }

  int get liveConnections => _links.where((l) => !l.dead).length;

  Future<void> close() async {
    stopChaos();
    cutAll();
    await _server.close();
    await _frameEvents.close();
  }

  String dump() => frames.map((f) => '#${f.connection} $f').join('\n');
}

final class _Link {
  _Link(this.client, this.upstream, FaultProxy proxy)
      : _toUpstream = _Pipe(upstream, proxy._random),
        _toClient = _Pipe(client, proxy._random);
  final Socket client;
  final Socket upstream;
  final _Pipe _toUpstream;
  final _Pipe _toClient;
  bool dead = false;

  _Pipe pipe(Dir dir) => dir == Dir.c2s ? _toUpstream : _toClient;

  void destroy() {
    if (dead) return;
    dead = true;
    _toUpstream.stop();
    _toClient.stop();
    client.destroy();
    upstream.destroy();
  }

  void destroyUpstream() {
    dead = true;
    _toUpstream.stop();
    _toClient.stop();
    upstream.destroy();
  }
}

/// One direction of a link: writes immediately, or through an ordered,
/// time-scheduled queue when the direction is shaped.
final class _Pipe {
  _Pipe(this._socket, this._random);
  final Socket _socket;
  final Random _random;
  final _queue = Queue<(DateTime, Uint8List)>();
  DateTime _lastRelease = DateTime.fromMillisecondsSinceEpoch(0);
  bool _pumping = false;
  bool _stopped = false;

  void send(Uint8List bytes, LinkConditions cond) {
    if (_stopped) return;
    _cond = cond;
    if (!cond.shaping && _queue.isEmpty) {
      _write(bytes);
      return;
    }
    // A throttled link delivers progressively, one TCP segment at a time.
    final seg =
        cond.maxSegment ?? (cond.bytesPerSecond != null ? 1460 : bytes.length);
    for (var o = 0; o < bytes.length; o += seg) {
      final piece = Uint8List.sublistView(
          bytes, o, o + seg > bytes.length ? bytes.length : o + seg);
      var release = DateTime.now().add(cond.latency);
      if (cond.jitter > Duration.zero) {
        release = release.add(Duration(
            microseconds: _random.nextInt(cond.jitter.inMicroseconds + 1)));
      }
      final bps = cond.bytesPerSecond;
      if (bps != null) {
        final tx = Duration(microseconds: piece.length * 1000000 ~/ bps);
        final earliest = _lastRelease.add(tx);
        if (earliest.isAfter(release)) release = earliest;
      }
      if (release.isBefore(_lastRelease)) release = _lastRelease;
      if (cond.maxSegment != null && !release.isAfter(_lastRelease)) {
        // Keep segments in separate reads on the receiving side.
        release = _lastRelease.add(const Duration(milliseconds: 1));
      }
      _lastRelease = release;
      _queue.add((release, Uint8List.fromList(piece)));
    }
    _pump();
  }

  LinkConditions? _cond;

  Future<void> _pump() async {
    if (_pumping) return;
    _pumping = true;
    while (_queue.isNotEmpty && !_stopped) {
      if (_cond?.paused ?? false) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
        continue;
      }
      final (at, bytes) = _queue.first;
      final wait = at.difference(DateTime.now());
      if (wait > Duration.zero) await Future<void>.delayed(wait);
      if (_stopped) break;
      _queue.removeFirst();
      _write(bytes);
    }
    _pumping = false;
  }

  void _write(Uint8List bytes) {
    try {
      _socket.add(bytes);
    } on Object {
      _stopped = true;
    }
  }

  void stop() {
    _stopped = true;
    _queue.clear();
  }
}
