import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart' show addTearDown;

import 'mosquitto.dart';

/// A third-party MQTT 5 client (mosquitto_sub) used as an independent witness
/// of what the broker forwards. Its JSON output includes every v5 property.
final class MosqSub {
  MosqSub._(this._process);

  final Process _process;
  final List<Map<String, dynamic>> messages = [];
  final StringBuffer output = StringBuffer();
  final Completer<void> _subscribed = Completer<void>();

  static Future<MosqSub> start(
    int port,
    List<String> topics, {
    int qos = 2,
    List<String> extra = const [],
  }) async {
    // mosquitto_sub block-buffers stdout on a pipe; a pty makes it line
    // buffered so messages are observed as they arrive.
    final binary = requireMosquittoProgram('mosquitto_sub');
    final process = await Process.start('script', [
      '-q',
      '/dev/null',
      binary,
      '-V',
      'mqttv5',
      '-h',
      '127.0.0.1',
      '-p',
      '$port',
      '-q',
      '$qos',
      '-d',
      '-F',
      '%j',
      for (final t in topics) ...['-t', t],
      ...extra,
    ]);
    final sub = MosqSub._(process);
    // Stopped even when the test fails before its own stop() call.
    addTearDown(sub.stop);
    process.stdout
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen(sub._onLine);
    process.stderr
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen((l) => sub.output.writeln('[stderr] $l'));
    try {
      await sub._subscribed.future.timeout(const Duration(seconds: 10));
    } on TimeoutException {
      await sub.stop();
      throw TimeoutException('mosquitto_sub did not subscribe:\n${sub.output}');
    }
    return sub;
  }

  void _onLine(String raw) {
    final line = raw.replaceAll('\r', '');
    output.writeln(line);
    if (line.contains('received SUBACK') && !_subscribed.isCompleted) {
      _subscribed.complete();
    }
    if (line.startsWith('{')) {
      messages.add(jsonDecode(line) as Map<String, dynamic>);
    }
  }

  Future<void> waitFor(int count,
          {Duration timeout = const Duration(seconds: 10)}) =>
      waitUntil(() => messages.length >= count,
          timeout: timeout,
          reason: 'mosquitto_sub got ${messages.length}/$count\n$output');

  Future<void> stop() async {
    _process.kill();
    await _process.exitCode;
  }
}

/// Publishes one message with the third-party mosquitto_pub client.
Future<void> mosqPub(
  int port,
  String topic,
  String message, {
  int qos = 0,
  bool retain = false,
  List<String> extra = const [],
}) async {
  final r = await Process.run(requireMosquittoProgram('mosquitto_pub'), [
    '-V',
    'mqttv5',
    '-h',
    '127.0.0.1',
    '-p',
    '$port',
    '-t',
    topic,
    '-m',
    message,
    '-q',
    '$qos',
    if (retain) '-r',
    ...extra,
  ]);
  if (r.exitCode != 0) {
    throw StateError('mosquitto_pub failed: ${r.stdout}${r.stderr}');
  }
}
