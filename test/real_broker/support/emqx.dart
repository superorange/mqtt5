import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'mosquitto.dart' show freePort;

/// A real EMQX broker in Docker, used as a second, independent MQTT 5 server
/// implementation. Containers are named with a test-specific prefix so they
/// cannot be confused with (or removed by) anything else on the machine.
final class Emqx {
  Emqx._(this.name, this.port);

  static const image = 'emqx/emqx:5.8.6';
  static const namePrefix = 'mqtt5-test-emqx-';

  final String name;
  final int port;

  static Future<bool> get available async {
    try {
      final r = await Process.run('docker', ['image', 'inspect', image]);
      return r.exitCode == 0;
    } on ProcessException {
      return false;
    }
  }

  /// Starts a container. [env] entries are EMQX config overrides such as
  /// `EMQX_MQTT__MAX_INFLIGHT=3`.
  static Future<Emqx> start({Map<String, String> env = const {}}) async {
    final suffix = '$pid-${Random().nextInt(1 << 32).toRadixString(16)}';
    final name = '$namePrefix$suffix';
    final port = await freePort();
    final r = await Process.run('docker', [
      'run',
      '-d',
      '--rm',
      '--name',
      name,
      '--label',
      'owner=mqtt5-tests',
      '-p',
      '127.0.0.1:$port:1883',
      for (final e in env.entries) ...['-e', '${e.key}=${e.value}'],
      image,
    ]);
    if (r.exitCode != 0) {
      throw StateError('docker run failed: ${r.stderr}');
    }
    final broker = Emqx._(name, port);
    await broker._waitReady();
    return broker;
  }

  Future<void> _waitReady() async {
    final deadline = DateTime.now().add(const Duration(seconds: 60));
    while (true) {
      final logs = await Process.run('docker', ['logs', name]);
      if ('${logs.stdout}${logs.stderr}'.contains('is running now')) {
        // The listener can lag the banner slightly.
        try {
          final s = await Socket.connect('127.0.0.1', port,
              timeout: const Duration(milliseconds: 500));
          s.destroy();
          return;
        } on SocketException {
          // Not yet.
        }
      }
      if (DateTime.now().isAfter(deadline)) {
        throw TimeoutException('EMQX did not start:\n${logs.stdout}');
      }
      await Future<void>.delayed(const Duration(milliseconds: 300));
    }
  }

  Future<String> logs() async {
    final r = await Process.run('docker', ['logs', name]);
    return '${r.stdout}${r.stderr}';
  }

  Future<void> dispose() async {
    await Process.run('docker', ['rm', '-f', name]);
  }
}
