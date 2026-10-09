import 'dart:async';
import 'dart:convert';
import 'dart:io';

import '../../support/mosquitto_tools.dart';

export '../../support/mosquitto_tools.dart';

/// A real mosquitto broker process, started on a free port with a generated
/// configuration. Nothing here imitates broker behaviour: every assertion made
/// against it is about what an actual MQTT 5 server did.
final class Mosquitto {
  Mosquitto._(this.dir, this.port, this._configLines);

  /// The broker program, or null when mosquitto is not installed — see
  /// `test/support/mosquitto_tools.dart` for how it is located.
  static String? get binary => mosquittoProgram('mosquitto', directory: 'sbin');

  static bool get available => binary != null;

  final Directory dir;
  final int port;
  final List<String> _configLines;
  Process? _process;
  final StringBuffer _log = StringBuffer();
  final _logLines = StreamController<String>.broadcast();
  Future<int>? _exit;

  /// Everything mosquitto logged (log_type all: every packet in and out).
  String get log => _log.toString();

  Stream<String> get logLines => _logLines.stream;

  /// Starts a broker. [config] lines are appended after the listener block.
  static Future<Mosquitto> start({
    List<String> config = const [],
    Map<String, String>? users,
    String? acl,
    bool persistence = false,
    int? port,
    List<String> listenerConfig = const [],
    Directory? dir,
    DynSec? dynsec,
  }) async {
    final workDir =
        dir ?? await Directory.systemTemp.createTemp('mqtt5_real_broker_');
    final brokerPort = port ?? await freePort();
    final lines = <String>[
      'per_listener_settings false',
      'listener $brokerPort 127.0.0.1',
      ...listenerConfig,
      'log_dest stdout',
      'log_type all',
      'log_timestamp false',
      'allow_anonymous ${users == null && dynsec == null ? 'true' : 'false'}',
      // mosquitto 2.1's built-in persistence restores no client sessions;
      // the bundled sqlite plugin does.
      if (persistence) ...[
        'plugin ${requireMosquittoPlugin('mosquitto_persist_sqlite.so')}',
        'plugin_opt_db_file ${workDir.path}/persist.sqlite',
        'plugin_opt_flush_period 0',
      ],
    ];
    if (users != null) {
      final passwd = File('${workDir.path}/passwd');
      await passwd.writeAsString('');
      for (final entry in users.entries) {
        final r = await Process.run(
          requireMosquittoProgram('mosquitto_passwd'),
          ['-b', passwd.path, entry.key, entry.value],
        );
        if (r.exitCode != 0) {
          throw StateError('mosquitto_passwd failed: ${r.stderr}');
        }
      }
      lines.add('password_file ${passwd.path}');
    }
    if (acl != null) {
      final aclFile = File('${workDir.path}/acl');
      await aclFile.writeAsString(acl);
      lines.add('acl_file ${aclFile.path}');
    }
    if (dynsec != null) {
      lines.addAll(await dynsec._write(workDir));
    }
    lines.addAll(config);
    final broker = Mosquitto._(workDir, brokerPort, lines);
    await broker._launch();
    return broker;
  }

  Future<void> _launch() async {
    final conf = File('${dir.path}/mosquitto.conf');
    await conf.writeAsString('${_configLines.join('\n')}\n');
    final process = await Process.start(
        requireMosquittoProgram('mosquitto', directory: 'sbin'),
        ['-c', conf.path]);
    _process = process;
    _exit = process.exitCode;
    process.stdout
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen((line) {
      _log.writeln(line);
      if (!_logLines.isClosed) _logLines.add(line);
    });
    process.stderr
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen((line) {
      _log.writeln('[stderr] $line');
      if (!_logLines.isClosed) _logLines.add(line);
    });
    final deadline = DateTime.now().add(const Duration(seconds: 10));
    while (true) {
      final exited = await _exit!.timeout(
        const Duration(milliseconds: 1),
        onTimeout: () => -999,
      );
      if (exited != -999) {
        throw StateError('mosquitto exited with $exited:\n$log');
      }
      try {
        final s = await Socket.connect('127.0.0.1', port,
            timeout: const Duration(milliseconds: 200));
        s.destroy();
        return;
      } on SocketException {
        if (DateTime.now().isAfter(deadline)) {
          throw StateError('mosquitto did not start listening:\n$log');
        }
        await Future<void>.delayed(const Duration(milliseconds: 20));
      }
    }
  }

  /// Waits until the broker log contains a line matching [pattern].
  Future<String> waitForLog(Pattern pattern,
      {Duration timeout = const Duration(seconds: 10)}) async {
    for (final line in const LineSplitter().convert(log)) {
      if (line.contains(pattern)) return line;
    }
    return logLines
        .firstWhere((line) => line.contains(pattern))
        .timeout(timeout, onTimeout: () {
      throw TimeoutException('No broker log line matching $pattern\n$log');
    });
  }

  /// Number of log lines matching [pattern].
  int countLog(Pattern pattern) => const LineSplitter()
      .convert(log)
      .where((line) => line.contains(pattern))
      .length;

  /// Graceful stop (SIGTERM): mosquitto sends DISCONNECT 0x8B to v5 clients.
  Future<void> stop() => _signal(ProcessSignal.sigterm);

  /// Crash (SIGKILL): sockets are reset, nothing is sent.
  Future<void> kill() => _signal(ProcessSignal.sigkill);

  Future<void> _signal(ProcessSignal signal) async {
    final process = _process;
    if (process == null) return;
    process.kill(signal);
    await _exit!.timeout(const Duration(seconds: 10), onTimeout: () {
      process.kill(ProcessSignal.sigkill);
      return _exit!;
    });
    _process = null;
  }

  /// Starts the broker again on the same port with the same configuration and
  /// working directory (so persistence, if enabled, carries over).
  Future<void> restart() async {
    await stop();
    _log.writeln('---- restart ----');
    await _launch();
  }

  Future<void> dispose() async {
    await kill();
    await _logLines.close();
    try {
      await dir.delete(recursive: true);
    } on FileSystemException {
      // Best effort.
    }
  }
}

Future<int> freePort() async {
  final s = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final port = s.port;
  await s.close();
  return port;
}

/// Polls [condition] until it holds.
Future<void> waitUntil(bool Function() condition,
    {Duration timeout = const Duration(seconds: 10), String? reason}) async {
  final deadline = DateTime.now().add(timeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('Condition not met: ${reason ?? ''}');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

/// Configuration for mosquitto's dynamic-security plugin, which (unlike
/// acl_file) can refuse SUBSCRIBE and UNSUBSCRIBE requests.
final class DynSec {
  DynSec({
    required this.username,
    required this.password,
    required this.acls,
    this.defaults = const {
      'publishClientSend': false,
      'publishClientReceive': true,
      'subscribe': false,
      'unsubscribe': true,
    },
  });

  final String username;
  final String password;

  /// Entries such as {'acltype': 'subscribePattern', 'topic': 'ok/#',
  /// 'allow': true}.
  final List<Map<String, Object>> acls;
  final Map<String, bool> defaults;

  static String get plugin =>
      requireMosquittoPlugin('mosquitto_dynamic_security.so');

  Future<List<String>> _write(Directory dir) async {
    final path = '${dir.path}/dynsec.json';
    final r = await Process.run(requireMosquittoProgram('mosquitto_ctrl'),
        ['dynsec', 'init', path, username, password]);
    if (r.exitCode != 0 || !File(path).existsSync()) {
      throw StateError('dynsec init failed: ${r.stdout}${r.stderr}');
    }
    final json =
        jsonDecode(await File(path).readAsString()) as Map<String, dynamic>;
    final role = (json['roles'] as List).first as Map<String, dynamic>;
    role['acls'] = acls;
    json['defaultACLAccess'] = defaults;
    await File(path).writeAsString(jsonEncode(json));
    return ['plugin $plugin', 'plugin_opt_config_file $path'];
  }
}
