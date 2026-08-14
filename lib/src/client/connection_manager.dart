import 'dart:async';

import '../codec/mqtt_packet_decoder.dart';
import '../exception/mqtt_exception.dart';
import '../logging/mqtt_logger.dart';
import '../packet/connack.dart';
import '../packet/connect.dart';
import '../packet/disconnect.dart';
import '../packet/mqtt_packet.dart';
import '../packet/mqtt_packet_codec.dart';
import '../packet/mqtt_reason_code.dart';
import '../packet/pingreq.dart';
import '../packet/pingresp.dart';
import '../property/mqtt_property.dart';
import '../transport/mqtt_transport.dart';
import 'keep_alive_manager.dart';
import 'mqtt_connection_state.dart';
import 'reconnect_manager.dart';

/// Manages the transport lifecycle: connecting, the CONNECT/CONNACK
/// handshake, the inbound packet loop, keep alive and reconnection.
final class ConnectionManager {
  ConnectionManager({
    required this.transportFactory,
    required this.onPacket,
    required this.onConnected,
    required this.onConnectionLost,
    this.connackTimeout = const Duration(seconds: 10),
    this.logger = const SilentLogger(),
    ReconnectManager? reconnectManager,
  }) : _reconnect = reconnectManager ?? ReconnectManager();

  final MqttTransport Function() transportFactory;
  final void Function(MqttPacket packet) onPacket;

  /// Called when the handshake completes and [MqttConnackPacket] was received
  /// with a success reason code.
  final void Function(MqttConnackPacket connack) onConnected;

  /// Called when an established connection is lost unexpectedly.
  final void Function() onConnectionLost;

  Duration connackTimeout;

  /// The maximum packet size the client may send (from the server CONNACK).
  int maximumPacketSize = 268435455;

  /// The maximum packet size the client will accept (declared in CONNECT).
  int clientMaximumPacketSize = 268435455;

  final MqttLogger logger;
  final ReconnectManager _reconnect;

  MqttTransport? _transport;
  MqttPacketDecoder _decoder = MqttPacketDecoder();
  StreamSubscription? _incomingSub;
  late KeepAliveManager _keepAlive;

  final StreamController<MqttConnectionState> _stateController =
      StreamController<MqttConnectionState>.broadcast(sync: true);
  MqttConnectionState _state = MqttConnectionState.disconnected;

  MqttConnectPacket? _connectPacket;
  Completer<MqttConnackPacket>? _connackCompleter;
  Timer? _connackTimer;
  bool _running = false;
  bool _runActive = false;

  MqttConnectionState get state => _state;

  Stream<MqttConnectionState> get stateStream => _stateController.stream;

  KeepAliveManager get keepAlive => _keepAlive;

  /// Starts the connect/reconnect loop using [connectPacket] for every
  /// (re)connection attempt.
  Future<void> start(MqttConnectPacket connectPacket) async {
    _connectPacket = connectPacket;
    if (_running) {
      return;
    }
    _running = true;
    _keepAlive = KeepAliveManager(
      onPingRequired: _onPingRequired,
      onPingTimeout: _onPingTimeout,
    );
    await _run();
  }

  /// Stops the loop and closes the transport. No reconnect is attempted.
  Future<void> stop() async {
    _running = false;
    _connackTimer?.cancel();
    _connackTimer = null;
    if (_connackCompleter != null && !_connackCompleter!.isCompleted) {
      _connackCompleter!.completeError(
        MqttConnectionException('Connection closed while connecting'),
      );
    }
    _keepAlive.stop();
    await _teardownTransport();
    _setState(MqttConnectionState.disconnected);
  }

  /// Sends a packet to the broker.
  void send(MqttPacket packet) {
    final transport = _transport;
    if (transport == null || !transport.isConnected) {
      throw MqttConnectionException('Not connected');
    }
    _write(packet);
  }

  /// Flushes outbound transport data.
  Future<void> flush() async {
    await _transport?.flush();
  }

  void _write(MqttPacket packet) {
    final transport = _transport;
    if (transport == null) {
      return;
    }
    final bytes = MqttPacketCodec.encode(packet);
    if (bytes.length > maximumPacketSize) {
      throw MqttPacketTooLargeException(
        'Packet of ${bytes.length} bytes exceeds the server maximum '
        'packet size of $maximumPacketSize',
      );
    }
    transport.add(bytes);
    _keepAlive.onOutboundActivity();
  }

  Future<void> _run() async {
    if (_runActive) {
      return;
    }
    _runActive = true;
    try {
      while (_running) {
        _setState(_reconnect.attempt == 0
            ? MqttConnectionState.connecting
            : MqttConnectionState.reconnecting);
        try {
          await _attemptConnect();
          _reconnect.reset();
          _setState(MqttConnectionState.connected);
          onConnected(_lastConnack!);
          _runActive = false;
          return;
        } on Object catch (e) {
          if (!_running) {
            break;
          }
          await _teardownTransport();
          _setState(MqttConnectionState.reconnecting);
          final delay = _reconnect.nextDelay();
          logger.log(
            MqttLogLevel.warning,
            'Connection failed (${_reconnect.attempt}): $e; '
            'retrying in ${delay.inMilliseconds} ms',
          );
          await Future<void>.delayed(delay);
        }
      }
    } finally {
      _runActive = false;
    }
  }

  MqttConnackPacket? _lastConnack;

  Future<void> _attemptConnect() async {
    final transport = transportFactory();
    await transport.connect();
    _transport = transport;
    _decoder = MqttPacketDecoder(maximumPacketSize: clientMaximumPacketSize);
    _incomingSub = transport.incoming.listen(
      _onData,
      onError: (Object error) => _onTransportError(error),
    );

    final connectPacket = _connectPacket;
    if (connectPacket == null) {
      throw MqttConnectionException('No CONNECT packet configured');
    }

    _connackCompleter = Completer<MqttConnackPacket>();
    _connackTimer = Timer(connackTimeout, _onConnackTimeout);
    _write(connectPacket);

    final connack = await _connackCompleter!.future;
    _connackTimer?.cancel();
    _connackTimer = null;
    _lastConnack = connack;

    if (connack.reasonCode != MqttReasonCode.success) {
      throw MqttServerRejectedException(
        connack.reasonCode.value,
        'Server rejected connection: ${connack.reasonCode.name}',
      );
    }

    _keepAlive.start(Duration(seconds: connectPacket.keepAliveSeconds));
    for (final property in connack.properties) {
      if (property is ServerKeepAlive) {
        _keepAlive.updateKeepAlive(Duration(seconds: property.seconds));
      }
    }
  }

  void _onConnackTimeout() {
    final completer = _connackCompleter;
    if (completer != null && !completer.isCompleted) {
      completer.completeError(
        MqttConnectionException('CONNACK timeout'),
      );
    }
  }

  void _onData(dynamic data) {
    if (!_running) {
      return;
    }
    try {
      final packets = _decoder.feed(data);
      for (final packet in packets) {
        if (_handleHandshakePacket(packet)) {
          continue;
        }
        if (packet is MqttPingrespPacket) {
          _keepAlive.onPingResponse();
          continue;
        }
        onPacket(packet);
      }
    } on MqttException catch (e) {
      logger.log(MqttLogLevel.error, 'Protocol error, closing connection: $e');
      _teardownTransport();
      _keepAlive.stop();
      onConnectionLost();
      unawaited(_run());
    }
  }

  bool _handleHandshakePacket(MqttPacket packet) {
    final completer = _connackCompleter;
    if (completer != null && !completer.isCompleted) {
      if (packet is MqttConnackPacket) {
        completer.complete(packet);
        return true;
      }
      if (packet is MqttDisconnectPacket) {
        completer.completeError(
          MqttServerRejectedException(
            packet.reasonCode?.value ?? 0,
            'Broker sent DISCONNECT during handshake',
          ),
        );
        return true;
      }
    }
    return false;
  }

  void _onTransportError(Object error) {
    if (!_running) {
      return;
    }
    _teardownTransport();
    _keepAlive.stop();
    onConnectionLost();
    unawaited(_run());
  }

  void _onPingRequired() {
    try {
      _write(const MqttPingreqPacket());
    } on MqttException catch (e) {
      logger.log(MqttLogLevel.warning, 'Failed to send PINGREQ: $e');
    }
  }

  void _onPingTimeout() {
    logger.log(MqttLogLevel.warning, 'Keep alive timeout');
    if (!_running) {
      return;
    }
    _teardownTransport();
    _keepAlive.stop();
    onConnectionLost();
    unawaited(_run());
  }

  Future<void> _teardownTransport() async {
    await _incomingSub?.cancel();
    _incomingSub = null;
    final transport = _transport;
    _transport = null;
    if (transport != null) {
      await transport.close();
    }
  }

  void _setState(MqttConnectionState state) {
    if (_state == state) {
      return;
    }
    _state = state;
    _stateController.add(state);
  }
}
