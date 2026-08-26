import 'dart:async';

/// Log levels for the MQTT client.
enum MqttLogLevel {
  trace,
  debug,
  info,
  warning,
  error,
  none,
}

/// Abstract logger used internally by the client.
abstract interface class MqttLogger {
  void log(MqttLogLevel level, String message);
}

/// A logger that discards everything (the default).
final class SilentLogger implements MqttLogger {
  const SilentLogger();

  @override
  void log(MqttLogLevel level, String message) {}
}

/// A logger that writes to [Zone] print, filtered by [minimumLevel].
final class PrintLogger implements MqttLogger {
  PrintLogger({this.minimumLevel = MqttLogLevel.info});

  final MqttLogLevel minimumLevel;

  @override
  void log(MqttLogLevel level, String message) {
    if (minimumLevel == MqttLogLevel.none ||
        level == MqttLogLevel.none ||
        level.index < minimumLevel.index) {
      return;
    }
    print('mqtt5 [${level.name}] $message');
  }
}
