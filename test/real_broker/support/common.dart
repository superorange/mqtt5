import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart' show addTearDown, fail;

import '../../support/mosquitto_tools.dart';

export 'fault_proxy.dart';
export 'mosquitto.dart';
export 'observer.dart';
export 'wire.dart';

/// Fast reconnects keep the fault-injection tests short.
ReconnectManager fastReconnect() => ReconnectManager(
      initialDelay: const Duration(milliseconds: 100),
      maxDelay: const Duration(milliseconds: 400),
      jitterFactor: 0,
    );

/// Collects library log lines so a failing test can show what happened.
final class CollectingLogger implements MqttLogger {
  final List<String> lines = [];
  @override
  void log(MqttLogLevel level, String message) =>
      lines.add('[${level.name}] $message');
  @override
  String toString() => lines.join('\n');
}

MqttClient newClient(
  int port, {
  String? clientId,
  bool autoReconnect = true,
  MqttWill? will,
  String? username,
  String? password,
  Duration operationTimeout = const Duration(seconds: 10),
  Duration? pingResponseTimeout,
  bool topicAliasEviction = false,
  MqttAuthenticator? authenticator,
  MqttLogger? logger,
  ReconnectManager? reconnectManager,
}) =>
    MqttClient(
      host: '127.0.0.1',
      port: port,
      clientId: clientId,
      autoReconnect: autoReconnect,
      will: will,
      username: username,
      password:
          password == null ? null : Uint8List.fromList(utf8.encode(password)),
      operationTimeout: operationTimeout,
      pingResponseTimeout: pingResponseTimeout,
      topicAliasEviction: topicAliasEviction,
      authenticator: authenticator,
      logger: logger ?? const SilentLogger(),
      reconnectManager: reconnectManager ?? fastReconnect(),
    );

Uint8List bytes(String s) => Uint8List.fromList(utf8.encode(s));
String text(Uint8List b) => utf8.decode(b);

/// Records every message a client receives.
final class Inbox {
  Inbox(MqttClient client) {
    _sub = client.messages.listen(messages.add);
  }
  late final StreamSubscription<MqttMessage> _sub;
  final List<MqttMessage> messages = [];

  List<String> get payloads => messages.map((m) => text(m.payload)).toList();

  Future<void> waitFor(int count,
      {Duration timeout = const Duration(seconds: 10)}) async {
    final deadline = DateTime.now().add(timeout);
    while (messages.length < count) {
      if (DateTime.now().isAfter(deadline)) {
        throw TimeoutException(
            'Inbox has ${messages.length}/$count: $payloads');
      }
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
  }

  Future<void> cancel() => _sub.cancel();
}

Future<void> settle([int ms = 300]) =>
    Future<void>.delayed(Duration(milliseconds: ms));

/// Compiles support/plugin/ext_auth.c into a temporary directory that is
/// removed when the current test (or group, from setUpAll) ends.
Future<String> buildAuthPlugin() async {
  final dir = await Directory.systemTemp.createTemp('mqtt5_plugin_');
  addTearDown(() => dir.delete(recursive: true));
  final so = '${dir.path}/ext_auth.so';
  final r = await Process.run('cc', [
    '-shared',
    '-fPIC',
    '-undefined',
    'dynamic_lookup',
    for (final dir in requireMosquittoPluginIncludeDirs()) '-I$dir',
    '-o',
    so,
    'test/real_broker/support/plugin/ext_auth.c',
  ]);
  if (r.exitCode != 0) fail('plugin build failed: ${r.stderr}');
  return so;
}
