import 'dart:async';
import 'dart:collection';
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
import '../packet/mqtt_pub_reply.dart';
import '../packet/mqtt_reason_code.dart';
import '../packet/pingreq.dart';
import '../packet/pingresp.dart';
import '../packet/suback.dart';
import '../packet/subscribe.dart';
import '../packet/unsuback.dart';
import '../packet/unsubscribe.dart';
import '../property/mqtt_property.dart';
import '../transport/mqtt_transport.dart';
import 'isolated_broadcast.dart';
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
    required this.onFatalError,
    this.onListenerError,
    this.autoReconnect = true,
    this.connackTimeout = const Duration(seconds: 10),
    this.pingResponseTimeout,
    this.logger = const SilentLogger(),
    ReconnectManager? reconnectManager,
  }) : _reconnect = reconnectManager ?? ReconnectManager();

  final MqttTransport Function() transportFactory;
  final void Function(MqttPacket packet) onPacket;

  /// Called when the connection ends for a reason no retry can fix, and no
  /// caller is awaiting [start] to receive the error.
  final void Function(Object error, StackTrace stackTrace) onFatalError;

  /// Called when a [stateStream] listener throws. The exception is not
  /// allowed to escape the state notification.
  final void Function(Object error, StackTrace stackTrace)? onListenerError;

  /// Whether a lost connection is re-established automatically.
  final bool autoReconnect;

  /// Called when the handshake completes and [MqttConnackPacket] was received
  /// with a success reason code. Awaited before the manager exposes
  /// [MqttConnectionState.connected], so session resume can finish first.
  final Future<void> Function(MqttConnackPacket connack) onConnected;

  /// Called when an established connection is lost.
  final void Function() onConnectionLost;

  Duration connackTimeout;

  /// How long the link may stay silent after a PINGREQ before it is declared
  /// lost. Null falls back to the keep alive interval.
  final Duration? pingResponseTimeout;

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

  /// Measures how long each connection lasted, for [ReconnectManager.stableAfter].
  final Stopwatch _uptime = Stopwatch();

  MqttTransport? _transport;
  MqttPacketDecoder _decoder = MqttPacketDecoder();
  StreamSubscription? _incomingSub;

  late final KeepAliveManager _keepAlive = KeepAliveManager(
    onPingRequired: _onPingRequired,
    onPingTimeout: _onPingTimeout,
    pingResponseTimeout: pingResponseTimeout,
  );

  late final IsolatedBroadcast<MqttConnectionState> _states =
      IsolatedBroadcast<MqttConnectionState>(
    onListenerError: _onStateListenerError,
  );
  MqttConnectionState _state = MqttConnectionState.disconnected;

  /// State changes made by a listener while another is being delivered.
  final Queue<MqttConnectionState> _pendingStates = Queue();
  bool _deliveringState = false;

  late MqttConnectPacket Function() _connectPacketBuilder;
  Completer<MqttConnackPacket>? _connackCompleter;
  Timer? _connackTimer;
  bool _running = false;
  Future<void>? _activeRun;
  Future<void>? _activeConnectionLoss;
  Completer<void>? _retryCancellation;
  bool _handshakeComplete = false;
  final List<MqttPacket> _deferredPackets = <MqttPacket>[];
  MqttConnectPacket? _connectPacket;
  MqttConnackPacket? _lastConnack;

  /// Set while a client-initiated re-authentication is waiting for the
  /// broker's AUTH 0x00 (section 4.12.1).
  Completer<void>? _reauthentication;

  /// The error that last ended the loop, for callers that joined it
  /// ([_join]). Cleared when a new loop starts.
  (Object, StackTrace)? _lastFatal;

  MqttConnectionState get state => _state;

  Stream<MqttConnectionState> get stateStream => _states.stream;

  /// Lets inbound packets other than the handshake CONNACK be dispatched.
  ///
  /// Called after session capabilities are applied and before inflight
  /// retransmission. Packets that arrived in the same read as CONNACK are
  /// flushed here. They cannot pile up beyond one read: the handshake
  /// completes in microtasks, before the next socket event is delivered.
  void acceptIncomingPackets() {
    _handshakeComplete = true;
    final deferred = List<MqttPacket>.from(_deferredPackets);
    _deferredPackets.clear();
    for (final packet in deferred) {
      // Thrown to the handshake caller ([_run]). Swallowing here and calling
      // [_failConnection] races that caller: this connect attempt fails with
      // the error, and [_run] sends the DISCONNECT and decides whether to
      // retry ([_shouldRetry]).
      _dispatchEstablished(packet);
    }
  }

  /// Starts the connect/reconnect loop, calling [connectPacketBuilder] for
  /// every (re)connection attempt.
  Future<void> start(MqttConnectPacket Function() connectPacketBuilder) async {
    _connectPacketBuilder = connectPacketBuilder;
    if (_running) {
      await _join();
      return;
    }
    _lastFatal = null;
    _reconnect.reset();
    _running = true;
    await _startRun(initial: true, backoffFirst: false);
  }

  /// Waits for the loop that is already running to produce a connection.
  ///
  /// Awaiting [_activeRun] is not enough: there is none while a lost
  /// connection is being torn down, and a reconnect run that ends for good
  /// completes normally (its error goes to [onFatalError]). Either way the
  /// caller would be told it is connected when it is not.
  Future<void> _join() async {
    if (_state != MqttConnectionState.connected) {
      await stateStream.firstWhere(
        (state) =>
            state == MqttConnectionState.connected ||
            state == MqttConnectionState.disconnected,
      );
    }
    if (_state == MqttConnectionState.connected) {
      return;
    }
    final fatal = _lastFatal;
    if (fatal != null) {
      Error.throwWithStackTrace(fatal.$1, fatal.$2);
    }
    throw MqttConnectionException('Connection closed while connecting');
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
    final activeRun = _activeRun;
    _running = false;
    final retryCancellation = _retryCancellation;
    if (retryCancellation != null && !retryCancellation.isCompleted) {
      retryCancellation.complete();
    }
    _connackTimer?.cancel();
    _connackTimer = null;
    final connack = _connackCompleter;
    if (connack != null && !connack.isCompleted) {
      connack.completeError(
        MqttConnectionException('Connection closed while connecting'),
      );
    }
    _keepAlive.stop();
    await _teardownTransport();
    await _activeConnectionLoss;
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
    await _states.close();
  }

  /// Sends a packet to the broker.
  void send(MqttPacket packet) {
    final transport = _transport;
    if (transport == null || !transport.isConnected) {
      throw MqttConnectionException('Not connected');
    }
    _write(packet);
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

  Future<void> _startRun({required bool initial, required bool backoffFirst}) {
    final active = _activeRun;
    if (active != null) {
      return active;
    }
    final run = _runTracked(initial: initial, backoffFirst: backoffFirst);
    _activeRun = run;
    return run;
  }

  Future<void> _runTracked({
    required bool initial,
    required bool backoffFirst,
  }) async {
    try {
      await _run(initial: initial, backoffFirst: backoffFirst);
    } finally {
      _activeRun = null;
    }
  }

  /// Runs the connect/reconnect loop.
  ///
  /// [initial] marks the run started by [start], whose caller is awaiting the
  /// result: errors that end the loop are rethrown to them. Runs started from
  /// a lost connection have no such caller, so their errors go to
  /// [onFatalError]. [backoffFirst] waits one backoff delay before the first
  /// attempt (after a server-initiated disconnect or a flapping connection).
  Future<void> _run({required bool initial, required bool backoffFirst}) async {
    var wait = backoffFirst;
    Object? lastError;
    StackTrace? lastStack;
    while (_running) {
      if (wait) {
        _setState(MqttConnectionState.reconnecting);
        final delay = _reconnect.nextDelay();
        logger.log(
          MqttLogLevel.warning,
          'Connection failed (${_reconnect.attempt}): '
          '${lastError ?? 'connection lost'}; '
          'retrying in ${delay.inMilliseconds} ms',
        );
        await _waitForRetry(delay);
        if (!_running) {
          break;
        }
      }
      wait = true;
      _setState(initial && _reconnect.attempt == 0
          ? MqttConnectionState.connecting
          : MqttConnectionState.reconnecting);
      var enteredOnConnected = false;
      try {
        await _attemptConnect();
        // onConnected calls acceptIncomingPackets, which flushes whatever
        // arrived in the same read as the CONNACK.
        enteredOnConnected = true;
        await onConnected(_lastConnack!);
        _uptime
          ..reset()
          ..start();
        _setState(MqttConnectionState.connected);
        return;
      } on Object catch (error, stackTrace) {
        lastError = error;
        lastStack = stackTrace;
        if (_running && !_shouldRetry(error, initial: initial)) {
          await _abort(
            error,
            stackTrace,
            rethrowToCaller: initial,
            connectionLost: enteredOnConnected,
          );
          return;
        }
        // The keep-alive timer was started for this attempt; it must not fire
        // during the backoff. A protocol error found after CONNACK is told to
        // the broker (section 4.13) before the socket goes.
        _keepAlive.stop();
        _sendDisconnectFor(error);
        await _teardownTransport();
        if (enteredOnConnected) {
          // The client side already treated this attempt as a connection:
          // capabilities applied, send quota opened, resubscriptions sent.
          onConnectionLost();
        }
      }
    }
    if (initial && lastError != null) {
      Error.throwWithStackTrace(lastError, lastStack!);
    }
  }

  /// Whether the connect loop should try again after [error].
  ///
  /// The call that [start]ed the loop ([initial]) gets every error that
  /// [isRetryableMqttConnectionError] does not clear: the caller is there to
  /// see it, and a bad setting should fail fast.
  ///
  /// An automatic reconnect has no caller, and the same settings were already
  /// accepted by this broker. Stopping it leaves an unattended client offline
  /// for good, so it also retries — with backoff — on:
  ///
  /// * a protocol error in the handshake, including one in a packet that
  ///   shared the CONNACK's read. Once connected, the same error only drops
  ///   the connection ([_failConnection]); one read earlier it must not end
  ///   the client.
  /// * CONNACK 0x85 (Client Identifier not valid) for the identifier the
  ///   broker accepted before. EMQX has been seen to answer reconnects this
  ///   way while it is overloaded.
  ///
  /// Still final: authentication failures, other CONNACK rejections
  /// (credentials, bans, unsupported will settings — the application has to
  /// change something), server redirection, a session this client does not
  /// own, and programming errors.
  bool _shouldRetry(Object error, {required bool initial}) {
    if (!autoReconnect) {
      return false;
    }
    if (isRetryableMqttConnectionError(error)) {
      return true;
    }
    if (initial) {
      return false;
    }
    if (error is MqttSessionNotOwnedException) {
      return false;
    }
    if (error is MqttProtocolException) {
      return true;
    }
    return error is MqttServerRejectedException &&
        error.reasonCode == MqttReasonCode.clientIdentifierNotValid.value;
  }

  Future<void> _waitForRetry(Duration delay) async {
    final cancellation = Completer<void>();
    _retryCancellation = cancellation;
    await Future.any<void>([
      Future<void>.delayed(delay),
      cancellation.future,
    ]);
    _retryCancellation = null;
  }

  /// Ends the loop for good: stops retrying, releases the transport and
  /// reports [error] exactly once.
  ///
  /// [connectionLost] notifies [onConnectionLost] after the teardown, for an
  /// attempt that failed after [onConnected] had started.
  Future<void> _abort(
    Object error,
    StackTrace stackTrace, {
    required bool rethrowToCaller,
    bool connectionLost = false,
  }) async {
    _running = false;
    _lastFatal = (error, stackTrace);
    _keepAlive.stop();
    _sendDisconnectFor(error);
    await _teardownTransport();
    if (connectionLost) {
      onConnectionLost();
    }
    _setState(MqttConnectionState.disconnected, force: true);
    if (rethrowToCaller) {
      Error.throwWithStackTrace(error, stackTrace);
    }
    _reportFatal(error, stackTrace);
  }

  /// Section 4.13 and section 4.12.1: before closing because of a protocol,
  /// authentication or timeout failure, tell the peer why. Best effort — a
  /// connection that is already gone has nothing to send on.
  void _sendDisconnectFor(Object error) {
    if (error is! MqttProtocolException &&
        error is! MqttAuthenticationException &&
        error is! MqttTimeoutException) {
      return;
    }
    try {
      _write(MqttDisconnectPacket(reasonCode: _disconnectReasonFor(error)));
    } on Object {
      // Nothing to report: the connection is ending either way.
    }
  }

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

      final connectPacket = _connectPacketBuilder();
      _connectPacket = connectPacket;

      _write(connectPacket);
      // MQTT-3.1.2-21 measures keep alive from the CONNECT write, not from
      // the moment CONNACK arrives. A slow handshake must not postpone the
      // first PINGREQ by a full interval.
      final connectSentAt = _keepAlive.monotonicNow;
      final connack = await _waitForConnack();
      _lastConnack = connack;

      _failOnRejection(connack);
      await _verifyAuthenticationOutcome(connack.properties, connack: true);

      _keepAlive.start(
        Duration(seconds: connectPacket.keepAliveSeconds),
        outboundAt: connectSentAt,
      );
      for (final property in connack.properties) {
        if (property is ServerKeepAlive) {
          _keepAlive.updateKeepAlive(Duration(seconds: property.seconds));
        }
      }
    } on Object catch (error) {
      _sendDisconnectFor(error);
      await _teardownTransport();
      rethrow;
    }
  }

  Future<MqttConnackPacket> _waitForConnack() async {
    final completer = Completer<MqttConnackPacket>();
    _connackCompleter = completer;
    _armConnackTimer();
    try {
      return await completer.future;
    } finally {
      _connackTimer?.cancel();
      _connackTimer = null;
      _connackCompleter = null;
    }
  }

  /// (Re)starts the CONNACK deadline. Every AUTH round restarts it, so a
  /// multi-step enhanced authentication is bounded per step, not in total.
  void _armConnackTimer() {
    _connackTimer?.cancel();
    _connackTimer = Timer(connackTimeout, () {
      final completer = _connackCompleter;
      if (completer != null && !completer.isCompleted) {
        completer.completeError(MqttConnectionException('CONNACK timeout'));
      }
    });
  }

  Future<void> _respondToHandshakeAuth(MqttAuthPacket auth) async {
    final completer = _connackCompleter!;
    _armConnackTimer();
    _setState(MqttConnectionState.authenticating);
    try {
      _validateServerAuth(auth);
      await _sendAuthResponse(auth);
    } on Object catch (error, stackTrace) {
      if (!completer.isCompleted) {
        completer.completeError(error, stackTrace);
      }
    }
  }

  /// The Authentication Method this connection was opened with, or null when
  /// enhanced authentication is not in use.
  String? get _connectAuthenticationMethod =>
      _propertyOf<AuthenticationMethod>(_connectPacket?.properties)?.value;

  /// Checks an AUTH packet the broker sent before acting on it.
  ///
  /// MQTT-4.12.0-6 forbids the server sending AUTH at all unless the CONNECT
  /// carried an Authentication Method, MQTT-4.12.0-5 pins that method for the
  /// whole connection, and Table 3-11 reserves reason code 0x19 for the
  /// client. An AUTH carrying no Authentication Method at all is tolerated:
  /// section 3.15.2.1 allows a bare `AUTH` with Remaining Length 0 to mean
  /// success.
  void _validateServerAuth(MqttAuthPacket auth) {
    final expected = _connectAuthenticationMethod;
    if (expected == null) {
      throw MqttProtocolException(
        'Broker sent AUTH but the CONNECT packet carried no Authentication '
        'Method',
      );
    }
    _checkMethod(auth.properties, expected, 'AUTH');
    if (auth.reasonCode == MqttReasonCode.reAuthenticate) {
      throw MqttProtocolException(
        'Broker sent AUTH with reason code 0x19, which only a client may send',
      );
    }
  }

  void _checkMethod(
    List<MqttProperty> properties,
    String? expected,
    String packet,
  ) {
    final method = _propertyOf<AuthenticationMethod>(properties)?.value;
    if (method != null && method != expected) {
      throw MqttProtocolException(
        'Broker sent $packet with Authentication Method "$method" instead of '
        '${expected == null ? 'none' : 'the agreed "$expected"'}',
      );
    }
  }

  /// Answers a broker AUTH challenge with the next round of authentication
  /// data. Only reached after [_validateServerAuth], so the CONNECT carried a
  /// method. The reply carries reason code 0x18 (MQTT-4.12.0-3) and the same
  /// method (MQTT-4.12.0-5).
  Future<void> _sendAuthResponse(MqttAuthPacket challenge) async {
    final authenticator = this.authenticator;
    if (authenticator == null) {
      throw MqttAuthenticationException(
        'Server sent AUTH but no authenticator is configured',
      );
    }
    final method = _connectAuthenticationMethod!;
    final data = _propertyOf<AuthenticationData>(challenge.properties)?.data;
    final MqttAuthResponse? response;
    try {
      response = await authenticator.authenticate(
        MqttAuthChallenge(method: method, data: data),
      );
    } on Object catch (error) {
      throw MqttAuthenticationException('Authenticator failed', error);
    }
    if (response == null) {
      throw MqttAuthenticationException('Client aborted authentication');
    }
    _write(
      MqttAuthPacket(
        reasonCode: MqttReasonCode.continueAuthentication,
        properties: [
          AuthenticationMethod(method),
          AuthenticationData(response.data),
        ],
      ),
    );
  }

  /// Checks the outcome of an enhanced authentication: the method echoed by
  /// the server must be the agreed one, and an [MqttAuthenticationVerifier]
  /// gets to check the server's final data before the result is trusted.
  Future<void> _verifyAuthenticationOutcome(
    List<MqttProperty> properties, {
    required bool connack,
  }) async {
    final method = _connectAuthenticationMethod;
    _checkMethod(properties, method, connack ? 'CONNACK' : 'AUTH');
    final verifier = authenticator;
    if (method == null || verifier is! MqttAuthenticationVerifier) {
      return;
    }
    try {
      await (verifier as MqttAuthenticationVerifier).verifyServer(
        MqttAuthChallenge(
          method: method,
          data: _propertyOf<AuthenticationData>(properties)?.data,
        ),
      );
    } on Object catch (error) {
      throw MqttAuthenticationException('Server verification failed', error);
    }
  }

  /// Starts a client-initiated re-authentication (section 4.12.1).
  ///
  /// Sends AUTH with reason code 0x19 and completes when the broker answers
  /// with AUTH 0x00. Other traffic keeps flowing throughout, as the section
  /// requires; only the reported state changes.
  Future<void> reauthenticate({
    Uint8List? authenticationData,
    Duration? timeout,
  }) async {
    if (_reauthentication != null) {
      throw MqttAuthenticationException(
        'A re-authentication is already in progress',
      );
    }
    if (_state != MqttConnectionState.connected) {
      throw MqttConnectionException('Not connected');
    }
    final method = _connectAuthenticationMethod;
    if (method == null) {
      // MQTT-4.12.0-7.
      throw MqttAuthenticationException(
        'Re-authentication requires an authenticationMethod on connect()',
      );
    }
    final completer = Completer<void>();
    _reauthentication = completer;
    _setState(MqttConnectionState.authenticating);
    try {
      // MQTT-4.12.1-1: the same Authentication Method as the original.
      _write(
        MqttAuthPacket(
          reasonCode: MqttReasonCode.reAuthenticate,
          properties: [
            AuthenticationMethod(method),
            if (authenticationData != null)
              AuthenticationData(authenticationData),
          ],
        ),
      );
      await (timeout == null
          ? completer.future
          : completer.future.timeout(
              timeout,
              onTimeout: () => throw MqttTimeoutException(
                'Timed out after ${timeout.inSeconds}s waiting for the '
                're-authentication to complete',
              ),
            ));
    } on MqttConnectionException {
      // The connection went away underneath the exchange. That is not a
      // failed authentication: the normal reconnect handling applies.
      rethrow;
    } on Object catch (error, stackTrace) {
      // MQTT-4.12.1-2: a failed re-authentication closes the connection.
      await _abort(error, stackTrace, rethrowToCaller: false);
      rethrow;
    } finally {
      if (identical(_reauthentication, completer)) {
        _reauthentication = null;
      }
      if (_state == MqttConnectionState.authenticating) {
        _setState(MqttConnectionState.connected);
      }
    }
  }

  void _failOnRejection(MqttConnackPacket connack) {
    final reasonCode = connack.reasonCode;
    if (reasonCode == MqttReasonCode.useAnotherServer ||
        reasonCode == MqttReasonCode.serverMoved) {
      throw MqttServerMovedException(
        reasonCode.value,
        _propertyOf<ServerReference>(connack.properties)?.value,
      );
    }
    if (reasonCode != MqttReasonCode.success) {
      throw MqttServerRejectedException(
        reasonCode.value,
        'Server rejected connection: ${reasonCode.name}',
      );
    }
  }

  static T? _propertyOf<T>(List<MqttProperty>? properties) {
    for (final property in properties ?? const <MqttProperty>[]) {
      if (property is T) {
        return property as T;
      }
    }
    return null;
  }

  void _onData(Uint8List data) {
    if (!_running) {
      return;
    }
    metrics.bytesReceived += data.length;
    _keepAlive.onInboundActivity();
    final decoder = _decoder;
    try {
      decoder.add(data);
      // One packet at a time: acting on each before decoding the next means a
      // malformed packet cannot take earlier, valid ones down with it, and
      // nothing after a DISCONNECT or a fatal error is processed.
      while (identical(decoder, _decoder) && _running) {
        final packet = decoder.nextPacket();
        if (packet == null) {
          break;
        }
        metrics.packetsReceived++;
        if (_handleHandshakePacket(packet)) {
          continue;
        }
        if (!_handshakeComplete) {
          _deferredPackets.add(packet);
          continue;
        }
        _dispatchEstablished(packet);
      }
    } on Object catch (e, stackTrace) {
      _failConnection(e, stackTrace);
    }
  }

  /// Treats [error] as a fault in the peer's byte stream and ends the
  /// connection, telling the broker why (section 4.13).
  void _failConnection(Object error, StackTrace stackTrace) {
    metrics.protocolErrorCount++;
    logger.log(
        MqttLogLevel.error, 'Protocol error, closing connection: $error');
    final completer = _connackCompleter;
    if (completer != null && !completer.isCompleted) {
      completer.completeError(error, stackTrace);
      return;
    }
    unawaited(
      _startConnectionLoss(
        error: error,
        stackTrace: stackTrace,
        disconnectReason: _disconnectReasonFor(error),
        serverInitiated: false,
      ),
    );
  }

  /// Control packets that only ever travel from client to server (Table 2-1),
  /// plus CONNACK, which a server sends exactly once and only as the first
  /// packet of a connection (MQTT-3.2.0-1, MQTT-3.2.0-2).
  static bool _isInvalidFromServer(MqttPacket packet) =>
      packet is MqttConnectPacket ||
      packet is MqttConnackPacket ||
      packet is MqttSubscribePacket ||
      packet is MqttUnsubscribePacket ||
      packet is MqttPingreqPacket;

  void _dispatchEstablished(MqttPacket packet) {
    if (_isInvalidFromServer(packet)) {
      throw MqttProtocolException(
        'Broker sent ${packet.type.name.toUpperCase()}, which a client must '
        'never receive',
      );
    }
    _checkProblemInformation(packet);
    if (packet is MqttPingrespPacket) {
      final sentAt = _pingSentAt;
      if (sentAt != null) {
        metrics.lastPingRtt = DateTime.now().difference(sentAt);
      }
      _keepAlive.onPingResponse();
      return;
    }
    if (packet is MqttAuthPacket) {
      unawaited(_handleReAuth(packet));
      return;
    }
    onPacket(packet);
  }

  /// MQTT-3.1.2-29: with Request Problem Information 0 the server may send a
  /// Reason String or User Properties only in PUBLISH, CONNACK and DISCONNECT.
  void _checkProblemInformation(MqttPacket packet) {
    final rpi =
        _propertyOf<RequestProblemInformation>(_connectPacket?.properties);
    if (rpi == null || rpi.value != 0) {
      return;
    }
    final List<MqttProperty> properties;
    if (packet is MqttPubReplyPacket) {
      properties = packet.properties;
    } else if (packet is MqttSubackPacket) {
      properties = packet.properties;
    } else if (packet is MqttUnsubackPacket) {
      properties = packet.properties;
    } else if (packet is MqttAuthPacket) {
      properties = packet.properties;
    } else {
      return;
    }
    if (properties.any((p) => p is ReasonString || p is UserProperty)) {
      throw MqttProtocolException(
        'Broker sent a Reason String or User Property in '
        '${packet.type.name.toUpperCase()} although the client requested no '
        'problem information',
      );
    }
  }

  /// The reason code the client puts in its DISCONNECT for [error]. Only
  /// codes Table 3-10 lets a client send are used.
  static MqttReasonCode _disconnectReasonFor(Object error) {
    if (error is MqttMalformedPacketException) {
      return MqttReasonCode.malformedPacket;
    }
    if (error is MqttTopicAliasInvalidException) {
      return MqttReasonCode.topicAliasInvalid;
    }
    if (error is MqttPacketTooLargeException) {
      return MqttReasonCode.packetTooLarge;
    }
    if (error is MqttReceiveMaximumExceededException) {
      return MqttReasonCode.receiveMaximumExceeded;
    }
    if (error is MqttProtocolException) {
      return MqttReasonCode.protocolError;
    }
    return MqttReasonCode.unspecifiedError;
  }

  /// Tears the connection down and, if enabled, restarts the connect loop.
  Future<void> _startConnectionLoss({
    required Object error,
    required StackTrace stackTrace,
    required bool serverInitiated,
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
      serverInitiated: serverInitiated,
    );
    _activeConnectionLoss = loss;
    return loss;
  }

  Future<void> _handleConnectionLoss({
    required Object error,
    required StackTrace stackTrace,
    required bool serverInitiated,
    MqttReasonCode? disconnectReason,
  }) async {
    var reconnect = false;
    var backoff = false;
    try {
      if (!_running) {
        return;
      }
      // Handshake still open, or the broker sent DISCONNECT: exponential,
      // capped by [ReconnectManager.maxDelay]. A network drop inside
      // [ReconnectManager.flapWindow] keeps climbing (accept-and-drop).
      // One that lasted [ReconnectManager.stableAfter] reconnects at once.
      // Between those two, wait a single initial delay.
      final wasUp = _uptime.isRunning;
      final lived = wasUp ? _uptime.elapsed : Duration.zero;
      _uptime.stop();
      final pace = _reconnect.paceFor(
        handshakeComplete: _handshakeComplete,
        serverInitiated: serverInitiated,
        lived: lived,
      );
      backoff = pace != ReconnectPace.immediate;
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
        _lastFatal = (error, stackTrace);
        _setState(MqttConnectionState.disconnected, force: true);
        _reportFatal(error, stackTrace);
        return;
      }
      reconnect = true;
    } finally {
      _activeConnectionLoss = null;
    }
    if (reconnect && _running) {
      metrics.reconnectCount++;
      await _startRun(initial: false, backoffFirst: backoff);
    }
  }

  Future<void> _handleReAuth(MqttAuthPacket auth) async {
    try {
      _validateServerAuth(auth);
    } on Object catch (error, stackTrace) {
      _failConnection(error, stackTrace);
      return;
    }
    final reauthentication = _reauthentication;
    // Section 3.15.2.1: an AUTH with Remaining Length 0 means Success.
    final reasonCode = auth.reasonCode ?? MqttReasonCode.success;
    try {
      if (reasonCode == MqttReasonCode.success) {
        await _verifyAuthenticationOutcome(auth.properties, connack: false);
        if (reauthentication != null && !reauthentication.isCompleted) {
          reauthentication.complete();
        }
        return;
      }
      await _sendAuthResponse(auth);
    } on Object catch (error, stackTrace) {
      if (reauthentication != null && !reauthentication.isCompleted) {
        // The caller of reauthenticate() closes the connection.
        reauthentication.completeError(error, stackTrace);
        return;
      }
      // A server-initiated exchange nobody is waiting on: MQTT-4.12.1-2.
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
      unawaited(_respondToHandshakeAuth(packet));
      return true;
    }
    // MQTT-3.2.0-1: the first packet the server sends on a connection must be
    // a CONNACK. Only the AUTH exchange and an outright rejection may come
    // first.
    throw MqttProtocolException(
      'Broker sent ${packet.type.name.toUpperCase()} before CONNACK',
    );
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
      _startConnectionLoss(
        error: error,
        stackTrace: stackTrace,
        serverInitiated: false,
      ),
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
    final diagnostic = _serverDisconnectDiagnostic(disconnect);
    logger.log(MqttLogLevel.warning, 'Broker sent DISCONNECT: $diagnostic');
    if (reasonCode != null && _isFatalDisconnectReason(reasonCode)) {
      await _abort(
        reasonCode == MqttReasonCode.useAnotherServer ||
                reasonCode == MqttReasonCode.serverMoved
            ? MqttServerMovedException(
                reasonCode.value,
                _propertyOf<ServerReference>(disconnect.properties)?.value,
              )
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
      serverInitiated: true,
    );
  }

  /// Replaces a live connection that has stopped doing its job — the broker
  /// keeps it open but never answers an outstanding QoS 1/2 exchange.
  ///
  /// MQTT-4.4.0-1 allows retransmission only on a new connection, so this is
  /// the one way to get the message re-sent. The connection is closed with
  /// DISCONNECT 0x00, which keeps the session (its expiry interval still
  /// applies) and suppresses the Will — the client is not gone. The normal
  /// reconnect path follows; without [autoReconnect], [error] ends the client
  /// as any lost connection would.
  void recycle(Object error, StackTrace stackTrace) {
    if (!_running || !_handshakeComplete) {
      return;
    }
    unawaited(
      _startConnectionLoss(
        error: error,
        stackTrace: stackTrace,
        serverInitiated: false,
        disconnectReason: MqttReasonCode.success,
      ),
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

  /// Reason codes after which reconnecting to the same server is pointless
  /// or harmful. 0x8E (Session taken over) belongs here: another connection
  /// now owns the client identifier, and taking it back would make the two
  /// evict each other forever.
  static bool _isFatalDisconnectReason(MqttReasonCode reasonCode) {
    return reasonCode == MqttReasonCode.useAnotherServer ||
        reasonCode == MqttReasonCode.serverMoved ||
        reasonCode == MqttReasonCode.notAuthorized ||
        reasonCode == MqttReasonCode.sessionTakenOver;
  }

  void _onPingRequired() {
    _pingSentAt = DateTime.now();
    try {
      _write(const MqttPingreqPacket());
    } on Object catch (error, stackTrace) {
      // A transport that can no longer write is a lost connection.
      logger.log(MqttLogLevel.warning, 'Failed to send PINGREQ: $error');
      unawaited(_startConnectionLoss(
        error: error,
        stackTrace: stackTrace,
        serverInitiated: false,
      ));
    }
  }

  void _onPingTimeout() {
    final waited = _keepAlive.effectivePingResponseTimeout;
    final message =
        'No traffic within ${waited.inMilliseconds} ms of PINGREQ; connection '
        'is dead';
    logger.log(MqttLogLevel.warning, message);
    unawaited(
      _startConnectionLoss(
        error: MqttConnectionException(message),
        stackTrace: StackTrace.current,
        serverInitiated: false,
      ),
    );
  }

  Future<void> _teardownTransport() async {
    _handshakeComplete = false;
    _deferredPackets.clear();
    _connectPacket = null;
    // Maximum Packet Size is negotiated per network connection (section
    // 3.1.2.11.4); a new attempt must not be policed by the old server's limit.
    maximumPacketSize = 268435455;
    // A re-authentication only exists for the life of one network connection,
    // so its caller has to be released rather than left waiting for an AUTH
    // that can no longer arrive.
    final reauthentication = _reauthentication;
    _reauthentication = null;
    if (reauthentication != null && !reauthentication.isCompleted) {
      reauthentication.completeError(
        MqttConnectionException('Connection lost during re-authentication'),
        StackTrace.current,
      );
    }
    final incomingSub = _incomingSub;
    _incomingSub = null;
    final transport = _transport;
    _transport = null;
    if (incomingSub != null) {
      try {
        await incomingSub.cancel();
      } on Object catch (error) {
        // A custom transport's stream may fail its cancel; the teardown must
        // still release the transport.
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
    } on Object catch (error) {
      logger.log(MqttLogLevel.warning, 'Transport close failed: $error');
    }
  }

  void _reportFatal(Object error, StackTrace stackTrace) {
    logger.log(MqttLogLevel.error, 'Connection ended: $error');
    onFatalError(error, stackTrace);
  }

  void _onStateListenerError(Object error, StackTrace stackTrace) {
    final report = onListenerError;
    if (report != null) {
      report(error, stackTrace);
      return;
    }
    logger.log(MqttLogLevel.error, 'State listener failed: $error');
  }

  /// Publishes a state change.
  ///
  /// State listeners run synchronously and may themselves change the state
  /// (for example by calling `disconnect()` from a listener). Such changes are
  /// queued and delivered after the current one instead of re-entering the
  /// controller. Once the manager has stopped, only [force]d or terminal
  /// transitions are published, so a late callback from a torn-down attempt
  /// cannot report a state that no longer exists.
  void _setState(MqttConnectionState state, {bool force = false}) {
    if (!_running &&
        !force &&
        state != MqttConnectionState.disconnected &&
        state != MqttConnectionState.disconnecting) {
      return;
    }
    if (_state == state) {
      return;
    }
    _state = state;
    _pendingStates.add(state);
    if (_deliveringState) {
      return;
    }
    _deliveringState = true;
    try {
      while (_pendingStates.isNotEmpty) {
        final next = _pendingStates.removeFirst();
        if (!_states.isClosed) {
          _states.add(next);
        }
      }
    } finally {
      _deliveringState = false;
    }
  }
}
