/// Locates the mosquitto installation the tests drive as real processes: the
/// broker, the third-party `mosquitto_sub` / `mosquitto_pub` clients used as
/// independent witnesses, the plugins a broker is started with, and the C
/// headers the test plugin is built against.
///
/// Nothing here assumes one machine's layout. The installation prefix comes
/// from `MQTT5_MOSQUITTO_PREFIX` when it is set, and is otherwise discovered
/// from `PATH` and then from the usual Homebrew and distro locations. A lookup
/// returns null when the piece is missing; the `require...` variants throw
/// instead, naming what is missing and the variable that points the suite at
/// the installation.

library;

import 'dart:io';

/// The installation's prefix, the directory holding its `sbin/` and `bin/`, or
/// null when mosquitto is not installed anywhere this can find it.
final String? mosquittoPrefix = _findPrefix();

/// A program shipped with the installation, or null when it is missing. The
/// broker lives in `sbin`; the clients and the tools live in `bin`.
String? mosquittoProgram(String name, {String directory = 'bin'}) {
  for (final dir in _searchDirectories(directory)) {
    final path = '$dir/$name';
    if (File(path).existsSync()) return path;
  }
  return null;
}

/// [mosquittoProgram] for a caller that cannot go on without the program.
String requireMosquittoProgram(String name, {String directory = 'bin'}) =>
    _require(mosquittoProgram(name, directory: directory), 'mosquitto $name');

/// A plugin (`mosquitto_*.so`) shipped with the installation, or null.
String? mosquittoPlugin(String name) {
  final prefix = mosquittoPrefix;
  if (prefix == null) return null;
  for (final path in [
    '$prefix/opt/mosquitto/lib/$name', // Homebrew installs it keg-only
    '$prefix/lib/$name',
    '$prefix/lib/mosquitto/$name',
  ]) {
    if (File(path).existsSync()) return path;
  }
  return null;
}

/// [mosquittoPlugin] for a caller that cannot go on without the plugin.
String requireMosquittoPlugin(String name) => _require(
    mosquittoPlugin(name), 'mosquitto plugin $name (minimal installs omit it)');

/// The include directories the test plugin is compiled with: mosquitto's
/// broker headers and cJSON's, which they include. A keg-only Homebrew install
/// keeps each under its own `opt/<formula>/include`; a linked one has both in
/// the prefix's `include`. Throws when either is missing, because the plugin
/// cannot be built without them.
List<String> requireMosquittoPluginIncludeDirs() {
  final prefix = mosquittoPrefix;
  final dirs = <String>[
    if (prefix != null) ...[
      '$prefix/opt/mosquitto/include', // Homebrew installs it keg-only
      '$prefix/opt/cjson/include', // the same, for mosquitto's dependency
      '$prefix/include',
    ],
  ].where((dir) => Directory(dir).existsSync()).toList();
  for (final header in ['mosquitto/broker_plugin.h', 'cjson/cJSON.h']) {
    if (!dirs.any((dir) => File('$dir/$header').existsSync())) {
      throw StateError("'$header' not found; install mosquitto's headers and "
          'cJSON, or set MQTT5_MOSQUITTO_PREFIX to the prefix they sit under');
    }
  }
  return dirs;
}

String _require(String? path, String what) {
  if (path == null) {
    throw StateError('$what not found; install mosquitto or set '
        'MQTT5_MOSQUITTO_PREFIX to the prefix it is installed under');
  }
  return path;
}

/// `<prefix>/bin` and `<prefix>/sbin`, then the keg-only Homebrew equivalents
/// under `opt/mosquitto/`. Both spellings are tried whichever one was asked
/// for, because installs differ on whether the broker sits in `sbin` or `bin`.
Iterable<String> _searchDirectories(String directory) sync* {
  final prefix = mosquittoPrefix;
  if (prefix == null) return;
  final other = directory == 'sbin' ? 'bin' : 'sbin';
  for (final base in ['$prefix/', '$prefix/opt/mosquitto/']) {
    yield '$base$directory';
    yield '$base$other';
  }
}

/// `MQTT5_MOSQUITTO_PREFIX`, else the prefix named by `PATH`, else the usual
/// locations. A configured prefix that holds no broker is an error rather than
/// a silent skip: the suite would otherwise look merely unsupported.
String? _findPrefix() {
  final configured = Platform.environment['MQTT5_MOSQUITTO_PREFIX'];
  if (configured != null && configured.isNotEmpty) {
    final prefix = configured.endsWith('/')
        ? configured.substring(0, configured.length - 1)
        : configured;
    if (!_holdsBroker(prefix)) {
      throw StateError('MQTT5_MOSQUITTO_PREFIX=$configured holds no mosquitto; '
          'point it at the directory that holds sbin/ and bin/');
    }
    return prefix;
  }
  for (final dir in (Platform.environment['PATH'] ?? '').split(':')) {
    // Either name reveals the prefix: the broker is in sbin, the clients are
    // in bin, and PATH may hold only one of the two.
    if (dir.startsWith('/') &&
        (File('$dir/mosquitto').existsSync() ||
            File('$dir/mosquitto_sub').existsSync())) {
      return File(dir).parent.path;
    }
  }
  for (final prefix in ['/opt/homebrew', '/usr/local', '/usr']) {
    if (_holdsBroker(prefix)) return prefix;
  }
  return null;
}

bool _holdsBroker(String prefix) => [
      '$prefix/sbin/mosquitto',
      '$prefix/bin/mosquitto',
      '$prefix/opt/mosquitto/sbin/mosquitto',
      '$prefix/opt/mosquitto/bin/mosquitto',
    ].any((path) => File(path).existsSync());
