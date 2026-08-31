import 'dart:async';
import 'dart:typed_data';

import '../codec/mqtt_packet_decoder.dart';
import '../exception/mqtt_exception.dart';
import '../logging/mqtt_logger.dart';
import '../packet/auth.dart';
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
import 'mqtt_authenticator.dart';
import 'mqtt_connection_state.dart';
import 'mqtt_metrics.dart';
import 'reconnect_manager.dart';

/// Manages the transport lifecycle: connecting, the CONNECT/CONNACK
/// handshake, the inbound packet loop, keep alive and reconnection.
final class ConnectionManager {
  ConnectionManager({
    required this.transportFactory,
    required this.onPacket,
    required this.onConnected,
    required this.onConnectionLost,
    this.onFatalError,
    this.autoReconnect = true,
    this.connackTimeout = const Duration(seconds: 10),
    this.logger = const SilentLogger(),
    ReconnectManager? reconnectManager,
  }) : _reconnect = reconnectManager ?? ReconnectManager();

  final MqttTransport Function() transportFactory;
  final void Function(MqttPacket packet) onPacket;

  /// Called when the connection ends for a reason no retry can fix, and no
  /// caller is awaiting [start] to receive the error.
  final void Function(Object error, StackTrace stackTrace)? onFatalError;

  /// Whether a lost connection is re-established automatically.
  final bool autoReconnect;

  /// Called when the handshake completes and [MqttConnackPacket] was received
  /// with a success reason code. Awaited before the manager exposes
  /// [MqttConnectionState.connected], so session resume can finish first.
  final Future<void> Function(MqttConnackPacket connack) onConnected;

  /// Called when an established connection is lost unexpectedly.
  final void Function() onConnectionLost;

  Duration connackTimeout;

  /// The maximum packet size the client may send (from the server CONNACK).
  int maximumPacketSize = 268435455;

  /// The maximum packet size the client will accept (declared in CONNECT).
  int clientMaximumPacketSize = 268435455;

  /// Handles enhanced authentication challenges. Set before [start].
  MqttAuthenticator? authenticator;

  final MqttLogger logger;
  final MqttMetrics metrics = MqttMetrics();
  DateTime? _pingSentAt;
  final ReconnectManager _reconnect;

  MqttTransport? _transport;
  MqttPacketDecoder _decoder = MqttPacketDecoder();
  StreamSubscription? _incomingSub;

  // Created lazily but unconditionally, so tearing down a connection that was
  // never started is safe.
  late final KeepAliveManager _keepAlive = KeepAliveManager(
    onPingRequired: _onPingRequired,
    onPingTimeout: _onPingTimeout,
  );

  final StreamController<MqttConnectionState> _stateController =
      StreamController<MqttConnectionState>.broadcast(sync: true);
  MqttConnectionState _state = MqttConnectionState.disconnected;

  MqttConnectPacket Function()? _connectPacketBuilder;
  Completer<MqttConnackPacket>? _connackCompleter;
  Timer? _connackTimer;
  bool _running = false;
  bool _disposed = false;
  Future<void>? _activeRun;
  Future<void>? _activeConnectionLoss;
  Completer<void>? _retryCancellation;
  bool _handshakeComplete = false;
  final List<MqttPacket> _deferredPackets = <MqttPacket>[];
  MqttConnectPacket? _connectPacket;

  /// Packets that arrive in the same TCP segment as CONNACK are held until
  /// session capabilities are applied. A compliant broker only needs a
  /// handful; a flood before the session is ready is a protocol error.
  static const int _maxDeferredPackets = 128;

  MqttConnectionState get state => _state;

  Stream<MqttConnectionState> get stateStream => _stateController.stream;

  KeepAliveManager get keepAlive => _keepAlive;

  /// Lets inbound packets other than the handshake CONNACK be dispatched.
  ///
  /// Called after session capabilities are applied and before inflight
  /// retransmission. Packets that arrived in the same TCP segment as
  /// CONNACK — including PUBACK/PUBCOMP that free Receive Maximum slots —
  /// are flushed here so resume cannot deadlock waiting on a deferred ack.
  void acceptIncomingPackets() {
    _handshakeComplete = true;
    _flushDeferredPackets();
  }

  /// Starts the connect/reconnect loop, calling [connectPacketBuilder] for
  /// every (re)connection attempt.
  Future<void> start(MqttConnectPacket Function() connectPacketBuilder) async {
    if (_disposed) {
      throw MqttConnectionException('Client has been closed');
    }
    _connectPacketBuilder = connectPacketBuilder;
    if (_running) {
      await _activeRun;
      return;
    }
    _reconnect.reset();
    _running = true;
    await _startRun(initial: true);
  }

  /// Marks the start of a client-initiated disconnect, so observers see the
  /// shutdown rather than an unexplained jump to [MqttConnectionState.disconnected].
  void beginDisconnect() {
    if (_running) {
      _setState(MqttConnectionState.disconnecting);
    }
  }

  /// Stops the loop and closes the transport. No reconnect is attempted.
  Future<void> stop() async {
    _running = false;
    final retryCancellation = _retryCancellation;
    if (retryCancellation != null && !retryCancellation.isCompleted) {
      retryCancellation.complete();
    }
    _connackTimer?.cancel();
    _connackTimer = null;
    if (_connackCompleter != null && !_connackCompleter!.isCompleted) {
      _connackCompleter!.completeError(
        MqttConnectionException('Connection closed while connecting'),
      );
    }
    _keepAlive.stop();
    await _teardownTransport();
    await _activeConnectionLoss;
    final activeRun = _activeRun;
    if (activeRun != null) {
      try {
        await activeRun;
      } on Object catch (error) {
        logger.log(
          MqttLogLevel.debug,
          'Connection loop stopped with an already reported error: $error',
        );
      }
    }
    _setState(MqttConnectionState.disconnected);
  }

  /// Releases resources held by the manager. The manager cannot be restarted.
  Future<void> dispose() async {
    await stop();
    _disposed = true;
    await _stateController.close();
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
    metrics.bytesSent += bytes.length;
    metrics.packetsSent++;
    _keepAlive.onOutboundActivity();
  }

  /// Runs the connect/reconnect loop.
  ///
  /// [initial] marks the run started by [start], whose caller is awaiting the
  /// result: errors that end the loop are rethrown to them. Runs started from
  /// a connection-lost callback have no such caller, so their errors go to
  /// [onFatalError] instead of escaping as unhandled asynchronous errors.
  Future<void> _startRun({required bool initial}) {
    final active = _activeRun;
    if (active != null) {
      return active;
    }
    final run = _runTracked(initial: initial);
    _activeRun = run;
    return run;
  }

  Future<void> _runTracked({required bool initial}) async {
    try {
      await _run(initial: initial);
    } finally {
      _activeRun = null;
    }
  }

  Future<void> _run({required bool initial}) async {
    while (_running) {
      _setState(initial && _reconnect.attempt == 0
          ? MqttConnectionState.connecting
          : MqttConnectionState.reconnecting);
      try {
        await _attemptConnect();
        _reconnect.reset();
        await onConnected(_lastConnack!);
        _flushDeferredPackets();
        _setState(MqttConnectionState.connected);
        return;
      } on Object catch (error, stackTrace) {
        if (!_running) {
          if (initial) {
            Error.throwWithStackTrace(error, stackTrace);
          }
          return;
        }
        if (_isFatal(error) || !autoReconnect) {
          await _abort(
            error,
            stackTrace,
            rethrowToCaller: initial,
          );
          return;
        }
        await _teardownTransport();
        if (!_running) {
          if (initial) {
            Error.throwWithStackTrace(error, stackTrace);
          }
          return;
        }
        _setState(MqttConnectionState.reconnecting);
        final delay = _reconnect.nextDelay();
        logger.log(
          MqttLogLevel.warning,
          'Connection failed (${_reconnect.attempt}): $error; '
          'retrying in ${delay.inMilliseconds} ms',
        );
        await _waitForRetry(delay);
        if (!_running) {
          if (initial) {
            Error.throwWithStackTrace(error, stackTrace);
          }
          return;
        }
      }
    }
  }

  Future<void> _waitForRetry(Duration delay) async {
    final cancellation = Completer<void>();
    _retryCancellation = cancellation;
    try {
      await Future.any<void>([
        Future<void>.delayed(delay),
        cancellation.future,
      ]);
    } finally {
      if (identical(_retryCancellation, cancellation)) {
        _retryCancellation = null;
      }
    }
  }

  /// Ends the loop for good: stops retrying, releases the transport and
  /// reports [error] exactly once.
  Future<void> _abort(
    Object error,
    StackTrace stackTrace, {
    required bool rethrowToCaller,
  }) async {
    _running = false;
    _keepAlive.stop();
    await _teardownTransport();
    _setState(MqttConnectionState.disconnected);
    if (rethrowToCaller) {
      Error.throwWithStackTrace(error, stackTrace);
    }
    _reportFatal(error, stackTrace);
  }

  bool _isFatal(Object error) {
    return !isRetryableMqttConnectionError(error);
  }

  MqttConnackPacket? _lastConnack;

  Future<void> _attemptConnect() async {
    final transport = transportFactory();
    try {
      await transport.connect();
    } on Object {
      await _closeTransport(transport);
      rethrow;
    }
    if (!_running) {
      await _closeTransport(transport);
      throw MqttConnectionException('Connection stopped');
    }
    _transport = transport;
    _handshakeComplete = false;
    _deferredPackets.clear();
    try {
      _decoder = MqttPacketDecoder(maximumPacketSize: clientMaximumPacketSize);
      _incomingSub =
          transport.incoming.listen(_onData, onError: _onTransportError);

      final builder = _connectPacketBuilder;
      if (builder == null) {
        throw MqttConnectionException('No CONNECT packet configured');
      }
      final connectPacket = builder();
      _connectPacket = connectPacket;

      _write(connectPacket);
      final connack = await _waitForConnack();
      _lastConnack = connack;

      _failOnRejection(connack);

      _keepAlive.start(Duration(seconds: connectPacket.keepAliveSeconds));
      for (final property in connack.properties) {
        if (property is ServerKeepAlive) {
          _keepAlive.updateKeepAlive(Duration(seconds: property.seconds));
        }
      }
    } on Object {
      await _teardownTransport();
      rethrow;
    }
  }

  Future<MqttConnackPacket> _waitForConnack() async {
    final completer = Completer<MqttConnackPacket>();
    _connackCompleter = completer;
    _connackTimer = Timer(connackTimeout, _onConnackTimeout);
    try {
      return await completer.future;
    } finally {
      _connackTimer?.cancel();
      _connackTimer = null;
      if (identical(_connackCompleter, completer)) {
        _connackCompleter = null;
      }
    }
  }

  Future<void> _respondToHandshakeAuth(MqttAuthPacket auth) async {
    final completer = _connackCompleter;
    if (completer == null || completer.isCompleted) {
      return;
    }
    _setState(MqttConnectionState.authenticating);
    try {
      await _sendAuthResponse(
        auth,
        responseReasonCode: MqttReasonCode.continueAuthentication,
      );
    } on Object catch (error, stackTrace) {
      if (!completer.isCompleted) {
        completer.completeError(error, stackTrace);
      }
    }
  }

  Future<void> _sendAuthResponse(
    MqttAuthPacket challenge, {
    required MqttReasonCode responseReasonCode,
  }) async {
    final authenticator = this.authenticator;
    if (authenticator == null) {
      throw MqttAuthenticationException(
        'Server sent AUTH but no authenticator is configured',
      );
    }
    final method = _propertyOf<AuthenticationMethod>(challenge)?.value ??
        _propertyOf<AuthenticationMethod>(_connectPacket)?.value;
    final data = _propertyOf<AuthenticationData>(challenge)?.data;
    final response = await authenticator.authenticate(
      MqttAuthChallenge(method: method, data: data),
    );
    if (response == null) {
      try {
        _write(
          MqttDisconnectPacket(reasonCode: MqttReasonCode.notAuthorized),
        );
      } on Object {
        // Best effort.
      }
      throw MqttAuthenticationException('Client aborted authentication');
    }
    _write(
      MqttAuthPacket(
        reasonCode: responseReasonCode,
        properties: [
          if (method != null) AuthenticationMethod(method),
          AuthenticationData(response.data),
        ],
      ),
    );
  }

  void _failOnRejection(MqttConnackPacket connack) {
    final reasonCode = connack.reasonCode;
    if (reasonCode == MqttReasonCode.useAnotherServer ||
        reasonCode == MqttReasonCode.serverMoved) {
      throw MqttServerMovedException(
        reasonCode.value,
        _propertyOf<ServerReference>(connack)?.value,
      );
    }
    if (reasonCode != MqttReasonCode.success) {
      throw MqttServerRejectedException(
        reasonCode.value,
        'Server rejected connection: ${reasonCode.name}',
      );
    }
  }

  T? _propertyOf<T>(MqttPacket? packet) {
    if (packet == null) {
      return null;
    }
    List<MqttProperty> properties;
    if (packet is MqttConnackPacket) {
      properties = packet.properties;
    } else if (packet is MqttConnectPacket) {
      properties = packet.properties;
    } else if (packet is MqttAuthPacket) {
      properties = packet.properties;
    } else {
      return null;
    }
    for (final property in properties) {
      if (property is T) {
        return property as T;
      }
    }
    return null;
  }

  void _onConnackTimeout() {
    final completer = _connackCompleter;
    if (completer != null && !completer.isCompleted) {
      completer.completeError(
        MqttConnectionException('CONNACK timeout'),
      );
    }
  }

  void _onData(Uint8List data) {
    if (!_running) {
      return;
    }
    metrics.bytesReceived += data.length;
    try {
      final packets = _decoder.feed(data);
      metrics.packetsReceived += packets.length;
      for (final packet in packets) {
        if (_handleHandshakePacket(packet)) {
          continue;
        }
        if (!_handshakeComplete) {
          if (_deferredPackets.length >= _maxDeferredPackets) {
            throw MqttProtocolException(
              'Too many packets queued before the session was ready '
              '(${_deferredPackets.length})',
            );
          }
          _deferredPackets.add(packet);
          continue;
        }
        _dispatchEstablished(packet);
      }
    } on Object catch (e, stackTrace) {
      // Anything thrown while decoding or dispatching a packet is a fault of
      // the peer's byte stream, including errors the codec did not classify.
      // Letting it escape would take down the enclosing zone.
      metrics.protocolErrorCount++;
      logger.log(MqttLogLevel.error, 'Protocol error, closing connection: $e');
      final completer = _connackCompleter;
      if (completer != null && !completer.isCompleted) {
        completer.completeError(e, stackTrace);
        return;
      }
      unawaited(
        _startConnectionLoss(
          error: e,
          stackTrace: stackTrace,
          disconnectReason: _disconnectReasonFor(e),
        ),
      );
    }
  }

  void _dispatchEstablished(MqttPacket packet) {
    if (packet is MqttPingrespPacket) {
      final sentAt = _pingSentAt;
      if (sentAt != null) {
        metrics.lastPingRtt = DateTime.now().difference(sentAt);
      }
      _keepAlive.onPingResponse();
      return;
    }
    if (packet is MqttAuthPacket) {
      unawaited(_handleReAuth(packet).catchError((Object e) {
        logger.log(MqttLogLevel.error, 'Re-authentication failed: $e');
      }));
      return;
    }
    onPacket(packet);
  }

  void _flushDeferredPackets() {
    final deferred = List<MqttPacket>.from(_deferredPackets);
    _deferredPackets.clear();
    for (final packet in deferred) {
      _dispatchEstablished(packet);
    }
  }

  MqttReasonCode _disconnectReasonFor(Object error) {
    if (error is MqttMalformedPacketException) {
      return MqttReasonCode.malformedPacket;
    }
    if (error is MqttPacketTooLargeException) {
      return MqttReasonCode.packetTooLarge;
    }
    if (error is MqttReceiveMaximumExceededException) {
      return MqttReasonCode.receiveMaximumExceeded;
    }
    return MqttReasonCode.protocolError;
  }

  /// Tears the connection down and, if enabled, restarts the connect loop.
  ///
  /// The teardown is awaited before reconnecting so a new transport cannot be
  /// installed while the old one is still being released.
  Future<void> _startConnectionLoss({
    required Object error,
    required StackTrace stackTrace,
    MqttReasonCode? disconnectReason,
  }) {
    final active = _activeConnectionLoss;
    if (active != null) {
      return active;
    }
    final loss = _handleConnectionLoss(
      error: error,
      stackTrace: stackTrace,
      disconnectReason: disconnectReason,
    );
    _activeConnectionLoss = loss;
    return loss;
  }

  Future<void> _handleConnectionLoss({
    required Object error,
    required StackTrace stackTrace,
    MqttReasonCode? disconnectReason,
  }) async {
    var reconnect = false;
    try {
      if (!_running) {
        return;
      }
      if (autoReconnect) {
        _setState(MqttConnectionState.reconnecting);
      }
      if (disconnectReason != null) {
        // Best effort: tell the broker why we are going away (section 4.13).
        try {
          _write(MqttDisconnectPacket(reasonCode: disconnectReason));
        } on Object {
          // The connection is already gone; nothing to report.
        }
      }
      _keepAlive.stop();
      await _teardownTransport();
      onConnectionLost();
      if (!_running) {
        return;
      }
      if (!autoReconnect) {
        _running = false;
        _setState(MqttConnectionState.disconnected);
        _reportFatal(error, stackTrace);
        return;
      }
      reconnect = true;
    } finally {
      _activeConnectionLoss = null;
    }
    if (reconnect && _running) {
      metrics.reconnectCount++;
      await _startRun(initial: false);
    }
  }

  Future<void> _handleReAuth(MqttAuthPacket auth) async {
    if (auth.reasonCode == MqttReasonCode.success) {
      return;
    }
    final responseCode = auth.reasonCode == MqttReasonCode.reAuthenticate
        ? MqttReasonCode.reAuthenticate
        : MqttReasonCode.continueAuthentication;
    if (auth.reasonCode != MqttReasonCode.reAuthenticate &&
        auth.reasonCode != MqttReasonCode.continueAuthentication) {
      return;
    }
    try {
      await _sendAuthResponse(auth, responseReasonCode: responseCode);
    } on MqttAuthenticationException catch (error, stackTrace) {
      await _abort(error, stackTrace, rethrowToCaller: false);
    }
  }

  bool _handleHandshakePacket(MqttPacket packet) {
    final completer = _connackCompleter;
    if (completer == null || completer.isCompleted) {
      return false;
    }
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
    if (packet is MqttAuthPacket) {
      unawaited(_respondToHandshakeAuth(packet).catchError((Object e) {
        logger.log(MqttLogLevel.error, 'Handshake AUTH failed: $e');
      }));
      return true;
    }
    return false;
  }

  void _onTransportError(Object error, StackTrace stackTrace) {
    if (!_running) {
      return;
    }
    logger.log(MqttLogLevel.warning, 'Transport error: $error');
    final connackCompleter = _connackCompleter;
    if (connackCompleter != null && !connackCompleter.isCompleted) {
      connackCompleter.completeError(error, stackTrace);
      return;
    }
    unawaited(
      _startConnectionLoss(error: error, stackTrace: stackTrace),
    );
  }

  /// Handles a DISCONNECT sent by the broker (specification section 3.14).
  ///
  /// The broker will not accept any further packets on this connection, so
  /// the transport is released immediately instead of waiting for the peer to
  /// close the socket.
  Future<void> handleServerDisconnect(MqttDisconnectPacket disconnect) async {
    if (!_running) {
      return;
    }
    final reasonCode = disconnect.reasonCode;
    String? serverReference;
    for (final property in disconnect.properties) {
      if (property is ServerReference) {
        serverReference = property.value;
      }
    }
    final diagnostic = _serverDisconnectDiagnostic(disconnect);
    logger.log(MqttLogLevel.warning, 'Broker sent DISCONNECT: $diagnostic');
    if (reasonCode != null && _isFatalDisconnectReason(reasonCode)) {
      await _abort(
        reasonCode == MqttReasonCode.useAnotherServer ||
                reasonCode == MqttReasonCode.serverMoved
            ? MqttServerMovedException(reasonCode.value, serverReference)
            : MqttServerRejectedException(
                reasonCode.value,
                'Broker closed the connection: $diagnostic',
              ),
        StackTrace.current,
        rethrowToCaller: false,
      );
      onConnectionLost();
      return;
    }
    await _startConnectionLoss(
      error: MqttConnectionException(
        'Broker closed the connection: $diagnostic',
      ),
      stackTrace: StackTrace.current,
    );
  }

  String _serverDisconnectDiagnostic(MqttDisconnectPacket disconnect) {
    final reasonCode = disconnect.reasonCode;
    String? reasonString;
    String? serverReference;
    final userProperties = <String>[];
    for (final property in disconnect.properties) {
      switch (property) {
        case ReasonString(:final value):
          reasonString = _safeDiagnosticValue(value);
        case ServerReference(:final value):
          serverReference = _safeDiagnosticValue(value);
        case UserProperty(:final name, :final value):
          if (userProperties.length < 8) {
            userProperties.add(
              '${_safeDiagnosticValue(name)}=${_safeDiagnosticValue(value)}',
            );
          }
        default:
          break;
      }
    }
    final value = reasonCode?.value;
    final code =
        value == null ? 'none' : '0x${value.toRadixString(16).padLeft(2, '0')}';
    return '${reasonCode?.name ?? 'noReason'} code=$code'
        '${reasonString == null ? '' : ' reasonString="$reasonString"'}'
        '${serverReference == null ? '' : ' serverReference="$serverReference"'}'
        '${userProperties.isEmpty ? '' : ' userProperties={${userProperties.join(', ')}}'}';
  }

  String _safeDiagnosticValue(String value) {
    final buffer = StringBuffer();
    for (final unit in value.codeUnits) {
      if (unit >= 0x20 && unit != 0x7F) {
        buffer.writeCharCode(unit);
      } else {
        buffer.write(' ');
      }
    }
    final singleLine = buffer.toString().trim();
    if (singleLine.length <= 256) return singleLine;
    return '${singleLine.substring(0, 256)}…';
  }

  /// Reason codes after which reconnecting to the same server is pointless.
  static bool _isFatalDisconnectReason(MqttReasonCode reasonCode) {
    return reasonCode == MqttReasonCode.useAnotherServer ||
        reasonCode == MqttReasonCode.serverMoved ||
        reasonCode == MqttReasonCode.notAuthorized ||
        reasonCode == MqttReasonCode.banned ||
        reasonCode == MqttReasonCode.badAuthenticationMethod;
  }

  void _onPingRequired() {
    try {
      _pingSentAt = DateTime.now();
      _write(const MqttPingreqPacket());
    } on Object catch (e) {
      logger.log(MqttLogLevel.warning, 'Failed to send PINGREQ: $e');
    }
  }

  void _onPingTimeout() {
    logger.log(MqttLogLevel.warning, 'Keep alive timeout');
    if (!_running) {
      return;
    }
    unawaited(
      _startConnectionLoss(
        error: MqttConnectionException('Keep alive timeout'),
        stackTrace: StackTrace.current,
      ),
    );
  }

  Future<void> _teardownTransport() async {
    _handshakeComplete = false;
    _deferredPackets.clear();
    _connectPacket = null;
    final incomingSub = _incomingSub;
    _incomingSub = null;
    final transport = _transport;
    _transport = null;
    if (incomingSub != null) {
      try {
        await incomingSub.cancel();
      } on Exception catch (error) {
        logger.log(
            MqttLogLevel.warning, 'Transport listener cancel failed: $error');
      }
    }
    if (transport != null) {
      await _closeTransport(transport);
    }
  }

  Future<void> _closeTransport(MqttTransport transport) async {
    try {
      await transport.close();
    } on Exception catch (error) {
      logger.log(MqttLogLevel.warning, 'Transport close failed: $error');
    }
  }

  void _reportFatal(Object error, StackTrace stackTrace) {
    logger.log(MqttLogLevel.error, 'Connection ended: $error');
    onFatalError?.call(error, stackTrace);
  }

  void _setState(MqttConnectionState state) {
    if (_state == state) {
      return;
    }
    _state = state;
    if (!_stateController.isClosed) {
      _stateController.add(state);
    }
  }
}
