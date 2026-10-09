import 'dart:async';
import 'dart:collection';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import '../codec/variable_byte_integer.dart';
import '../exception/mqtt_exception.dart';
import '../logging/mqtt_logger.dart';
import '../mqtt_qos.dart';
import '../packet/connack.dart';
import '../packet/connect.dart';
import '../packet/disconnect.dart';
import '../packet/mqtt_packet.dart';
import '../packet/puback.dart';
import '../packet/pubcomp.dart';
import '../packet/publish.dart';
import '../packet/pubrec.dart';
import '../packet/pubrel.dart';
import '../packet/mqtt_reason_code.dart';
import '../packet/suback.dart';
import '../packet/subscribe.dart';
import '../packet/unsuback.dart';
import '../packet/unsubscribe.dart';
import '../property/mqtt_property.dart';
import '../session/awaiting_ack.dart';
import '../session/mqtt_session.dart';
import '../session/outgoing_qos1.dart';
import '../session/outgoing_qos2.dart';
import '../session/topic_alias.dart';
import '../subscription.dart';
import '../topic.dart';
import '../transport/mqtt_transport.dart';
import '../transport/tcp_transport.dart';
import '../transport/tls_transport.dart';
import 'connection_manager.dart';
import 'flow_controller.dart';
import 'isolated_broadcast.dart';
import 'mqtt_authenticator.dart';
import 'mqtt_connection_state.dart';
import 'mqtt_message.dart';
import 'mqtt_metrics.dart';
import 'mqtt_publish_result.dart';
import 'reconnect_manager.dart';
import 'server_capabilities.dart';

/// A pure-Dart MQTT 5.0 client.
final class MqttClient {
  MqttClient({
    required this.host,
    this.port = 1883,
    String? clientId,
    this.useTls = false,
    this.securityContext,
    this.onBadCertificate,
    this.alpnProtocols,
    this.connectionTimeout = const Duration(seconds: 10),
    this.will,
    this.username,
    this.password,
    MqttLogger logger = const SilentLogger(),
    this.reconnectManager,
    this.authenticator,
    this.transportFactory,
    this.autoReconnect = true,
    this.operationTimeout = const Duration(seconds: 30),
    this.ackTimeout = const Duration(seconds: 60),
    this.pingResponseTimeout,
    this.topicAliasEviction = false,
  })  : logger = _GuardedMqttLogger(logger),
        clientId = clientId ?? _generateClientId() {
    if (port < 1 || port > 0xFFFF) {
      throw ArgumentError.value(port, 'port', 'Must be between 1 and 65535');
    }
    if (connectionTimeout <= Duration.zero) {
      throw ArgumentError.value(
        connectionTimeout,
        'connectionTimeout',
        'Must be greater than zero',
      );
    }
    if (operationTimeout < Duration.zero) {
      throw ArgumentError.value(
        operationTimeout,
        'operationTimeout',
        'Must not be negative',
      );
    }
    if (ackTimeout < Duration.zero) {
      throw ArgumentError.value(
        ackTimeout,
        'ackTimeout',
        'Must not be negative',
      );
    }
    final pingTimeout = pingResponseTimeout;
    if (pingTimeout != null && pingTimeout <= Duration.zero) {
      throw ArgumentError.value(
        pingTimeout,
        'pingResponseTimeout',
        'Must be greater than zero',
      );
    }
    final willTopic = will?.topic;
    if (willTopic != null) {
      // The Will Topic is a Topic Name (section 3.1.3.3). Rejecting it here
      // beats being disconnected by the broker on every connect attempt.
      final problem = MqttTopic.checkName(willTopic);
      if (problem != null) {
        throw ArgumentError.value(
            willTopic, 'will.topic', 'A will topic $problem');
      }
    }
    _connectionManager = ConnectionManager(
      transportFactory: _createTransport,
      onPacket: _onPacket,
      onConnected: _onConnected,
      onConnectionLost: _onConnectionLost,
      onFatalError: _onFatalError,
      onListenerError: _reportListenerError,
      autoReconnect: autoReconnect,
      pingResponseTimeout: pingResponseTimeout,
      logger: this.logger,
      reconnectManager: reconnectManager,
    );
  }

  final String host;
  final int port;
  final String clientId;
  final bool useTls;
  final Duration connectionTimeout;
  final MqttWill? will;
  final String? username;
  final Uint8List? password;
  final MqttLogger logger;
  final SecurityContext? securityContext;
  final bool Function(X509Certificate)? onBadCertificate;
  final List<String>? alpnProtocols;
  final ReconnectManager? reconnectManager;
  final MqttAuthenticator? authenticator;

  /// Whether a lost connection is re-established automatically.
  ///
  /// With `false`, [connect] fails on the first unsuccessful attempt and a
  /// connection that drops later is reported through [state] and [errors]
  /// without any retry.
  final bool autoReconnect;

  /// How long a call waits before failing with [MqttTimeoutException].
  ///
  /// The deadline covers [subscribe] and [unsubscribe] through the broker's
  /// acknowledgement, and the part of [publish] that waits for a Receive
  /// Maximum slot or a packet identifier. A QoS 1/2 publication already in
  /// the session store does not fail when this elapses: the message occupies
  /// a slot until PUBACK/PUBCOMP, a session loss, a [disconnect] that ends
  /// the session, or [close]. Failing it early would free the slot while the
  /// broker still considers the publication outstanding, and a retry would
  /// send a duplicate. [ackTimeout] is what bounds that wait.
  ///
  /// [Duration.zero] disables the timeout and waits indefinitely.
  final Duration operationTimeout;

  /// How long a QoS 1/2 exchange may wait for the broker's PUBACK, PUBREC or
  /// PUBCOMP on a live connection before that connection is replaced.
  ///
  /// Keep alive cannot notice a broker that keeps answering PINGREQ but has
  /// stopped acknowledging a publication. MQTT 5 allows a retransmission only
  /// on a new connection (MQTT-4.4.0-1), so when the oldest outstanding
  /// exchange exceeds this, the client closes the connection with
  /// DISCONNECT 0x00 (no Will) and reconnects. What happens to the waiting
  /// `publish` futures depends on the session:
  ///
  /// * With a session that outlives the connection (a non-zero
  ///   `sessionExpiryInterval`), the resumed session re-sends the publication
  ///   with DUP set and the future completes from that. Do not re-publish.
  /// * Without one, the reconnect starts a new session and the futures fail
  ///   with [MqttConnectionException], as on any lost session. Publishing
  ///   again is then the application's decision; the broker may already have
  ///   the message.
  /// * Without [autoReconnect], the client stops: the futures fail with
  ///   [MqttTimeoutException], which is also reported on [errors].
  ///
  /// Keep it well above the slowest acknowledgement the broker can produce
  /// under load. [Duration.zero] disables it, and a publication then waits
  /// until the broker answers or the connection drops.
  final Duration ackTimeout;

  /// How long the link may stay completely silent after a PINGREQ before the
  /// connection is treated as lost. Any inbound bytes — not only the PINGRESP
  /// — count as a sign of life, so a PINGRESP queued behind a large transfer
  /// on a slow link does not drop the connection.
  ///
  /// Null, the default, uses the keep alive interval itself. That makes the
  /// worst case for noticing a silently dropped link **two** intervals: one
  /// for the idle timer to send the PINGREQ, another for the reply that never
  /// arrives. At `keepAlive: 60s` that is two minutes; at `keepAlive: 300s` it
  /// is ten.
  ///
  /// Set this to decouple the two. `keepAlive` decides how often an idle
  /// connection must produce traffic — a floor the broker also enforces, and
  /// what a battery-powered client wants to keep long. This decides how
  /// quickly a dead link is noticed:
  ///
  /// ```dart
  /// MqttClient(
  ///   host: 'broker.example.com',
  ///   pingResponseTimeout: const Duration(seconds: 10),
  /// );
  /// await client.connect(keepAlive: const Duration(minutes: 5));
  /// ```
  ///
  /// Keep it comfortably above the round trip time to the broker: a value
  /// shorter than a slow network's latency turns ordinary delay into a
  /// reconnect loop.
  final Duration? pingResponseTimeout;

  /// Whether to evict the least recently used topic alias when all alias slots
  /// are occupied. Defaults to false.
  final bool topicAliasEviction;

  /// Overrides transport creation; intended for tests and custom transports.
  final MqttTransport Function()? transportFactory;

  late final ConnectionManager _connectionManager;

  /// Each listener is called directly and a throw is reported on [errors].
  /// A broadcast [StreamController] would let that throw escape [add]: async
  /// controllers surface it as an uncaught zone error, sync ones skip every
  /// listener registered after the one that threw.
  late final IsolatedBroadcast<MqttMessage> _messages =
      IsolatedBroadcast<MqttMessage>(
    onListen: _flushUndelivered,
    onListenerError: _reportListenerError,
  );

  /// Messages received while nobody listened to [messages], with the
  /// acknowledgement each still owes the broker.
  ///
  /// A broadcast stream drops events nobody listens to, and the broker was
  /// already told QoS 1/2 messages had arrived: the backlog of a resumed
  /// session, delivered right after CONNACK and before `await connect()`
  /// returns, vanished for good. Now nothing is acknowledged before it has
  /// reached a listener, so the broker's Receive Maximum holds further QoS 1/2
  /// traffic back while no one listens.
  final ListQueue<(MqttMessage, void Function()?)> _undelivered = ListQueue();

  /// QoS 0 messages in [_undelivered]. They cannot be held back by the
  /// broker, so they are capped; beyond the cap the oldest is dropped, which
  /// QoS 0 permits.
  int _undeliveredQos0 = 0;
  static const int _undeliveredQos0Limit = 1000;

  /// Identifies the current network connection. Acknowledgements owed for a
  /// message from an earlier connection are not sent on a later one: the
  /// broker re-sends what it did not get an acknowledgement for.
  int _epoch = 0;

  /// QoS 1 messages received on this connection whose PUBACK has not been
  /// sent yet; they count against the Receive Maximum declared in CONNECT.
  final Set<int> _pendingQos1Acks = <int>{};
  // `late` so the initializer may close over instance methods. A plain
  // field initializer runs before `this` exists.
  late final IsolatedBroadcast<Object> _errors = IsolatedBroadcast<Object>(
    onListenerError: _reportListenerError,
  );
  late final IsolatedBroadcast<MqttErrorEvent> _errorEvents =
      IsolatedBroadcast<MqttErrorEvent>(
    onListenerError: _reportListenerError,
  );
  bool _reportingListenerError = false;
  bool _undeliveredFlushScheduled = false;

  final MqttSession _session = MqttSession();
  final Map<int, _PendingSubscribe> _pendingSubscribes = {};
  final Map<int, Completer<MqttUnsubackPacket>> _pendingUnsubscribes = {};

  /// Topic-filter counts for outstanding UNSUBSCRIBE packets, so an UNSUBACK
  /// with the wrong number of reason codes can be rejected from the packet
  /// handler. Kept beside [_pendingUnsubscribes] so that map stays a plain
  /// `Map<int, Completer>` for [_failPending].
  final Map<int, int> _unsubscribeCounts = {};

  /// Publications a resumed session still has to re-send, oldest first.
  ///
  /// Belongs to the network connection rather than the session: it is rebuilt
  /// by every resume and dropped when the connection ends.
  final ListQueue<_ResumeItem> _resumeQueue = ListQueue<_ResumeItem>();

  bool _connected = false;
  bool _sessionPresent = false;
  bool _closed = false;

  /// True after this client instance has accepted a CONNACK and still holds
  /// the session that CONNACK described.
  ///
  /// Session state lives in this object's memory and is never saved. A new
  /// instance — in a new process or the same one — holds none, so a broker
  /// that reports Session Present to it is resuming a session it cannot
  /// continue (MQTT-3.2.2-4): the two sides would disagree about
  /// subscriptions and unacknowledged messages. Such a connect fails with
  /// [MqttSessionNotOwnedException] unless `adoptBrokerSession` is set.
  /// Reconnects of this instance keep the flag, including when nothing is in
  /// flight.
  bool _ownsSession = false;
  String? _assignedClientId;

  /// Whether a broker session this instance does not own may be taken over
  /// (`connect(adoptBrokerSession: true)`).
  bool _adoptBrokerSession = false;

  /// Monotonic clock for [ackTimeout].
  final Stopwatch _clock = Stopwatch()..start();

  /// Checks outstanding QoS 1/2 exchanges against [ackTimeout] while a
  /// connection is up.
  Timer? _ackWatch;

  /// Set while subscriptions re-sent after a lost session have not all been
  /// answered. A connection that ends in between may be followed by one that
  /// reports the new, empty session as present; without this the filters
  /// would never be sent again.
  bool _resubscribeIncomplete = false;

  /// Identifies the latest re-subscription round, so an older round that
  /// finishes late cannot clear [_resubscribeIncomplete].
  int _resubscribeRound = 0;

  final ServerCapabilities _capabilities = ServerCapabilities();
  List<MqttProperty> _connackProperties = const [];
  final FlowController _flow = FlowController();
  late final TopicAliasMap _outgoingAliases =
      TopicAliasMap(enableEviction: topicAliasEviction);
  final TopicAliasMap _incomingAliases = TopicAliasMap();
  int _clientReceiveMaximum = 65535;
  int _clientMaximumPacketSize = 268435455;
  int _clientTopicAliasMaximum = 0;

  // Connect settings, retained for reconnect and rebuilds.
  bool _cleanStart = true;
  bool? _reconnectCleanStart;
  bool _hasConnectedOnce = false;
  bool _lastSentCleanStart = true;
  Duration _keepAlive = const Duration(seconds: 60);
  Duration? _sessionExpiryInterval;
  List<MqttProperty> _connectProperties = const [];
  Duration _connackTimeout = const Duration(seconds: 10);
  String? _authenticationMethod;
  Uint8List? _authenticationData;

  MqttConnectionState get state => _connectionManager.state;

  Stream<MqttConnectionState> get stateStream => _connectionManager.stateStream;

  /// Whether the broker resumed a previous session on the last CONNACK.
  bool get sessionPresent => _sessionPresent;

  /// Invoked when the broker reports it has moved to another server.
  void Function(String? serverReference, MqttReasonCode reasonCode)?
      onServerMoved;

  /// Runtime diagnostics counters.
  MqttMetrics get metrics => _connectionManager.metrics;

  /// The number of QoS 1/2 publications currently awaiting acknowledgement.
  int get inflightCount => _session.inflightCount;

  /// Incoming application messages.
  ///
  /// A broadcast stream. Messages that arrive while it has no listener are
  /// kept, in order, until one subscribes — so listening after
  /// `await connect()` still sees a resumed session's backlog. QoS 1/2
  /// messages are acknowledged to the broker only once they have been handed
  /// to a listener; while nobody listens, the broker's Receive Maximum holds
  /// further QoS 1/2 traffic back. QoS 0 messages cannot be held back: at
  /// most 1000 are kept, the oldest dropped beyond that.
  ///
  /// A message counts as handed over when the listener's subscription takes
  /// it. A paused subscription — `await for` with an asynchronous body, for
  /// example — queues it, and it is acknowledged then, not when the
  /// asynchronous work finishes. Slow asynchronous processing therefore gets
  /// no backpressure from the broker; keep the handler synchronous, or bound
  /// the work it starts, when that matters.
  Stream<MqttMessage> get messages => _messages.stream;

  /// Errors that ended the connection for good.
  ///
  /// A failure on the initial [connect] is thrown from that call. Once the
  /// client is running, there is no caller left to throw to, so a rejection
  /// or an unrecoverable failure on a later reconnect is reported here.
  ///
  /// Failures an automatic reconnect retries (network errors, temporary
  /// rejections, a protocol error during the handshake, CONNACK 0x85) are not
  /// reported: they are logged at warning level while [state] stays
  /// [MqttConnectionState.reconnecting], and [MqttMetrics.reconnectCount]
  /// grows.
  Stream<Object> get errors => _errors.stream;

  /// Connection errors with the stack captured by the connection manager.
  /// The legacy [errors] stream emits the same error object without its stack.
  Stream<MqttErrorEvent> get errorEvents => _errorEvents.stream;

  /// The client identifier in use, which is the one the broker assigned in
  /// CONNACK when the client connected with an empty [clientId].
  String get effectiveClientId => _assignedClientId ?? clientId;

  /// The properties the broker sent in the most recent CONNACK.
  ///
  /// Empty before the first successful connection. Replaced on every
  /// (re)connection, because CONNACK properties are per-connection.
  List<MqttProperty> get connackProperties =>
      List.unmodifiable(_connackProperties);

  /// The Response Information returned by the broker, used as the prefix for
  /// request/response topics (specification section 4.10).
  ///
  /// Only present when the client sent `RequestResponseInformation(1)` in
  /// [connect]'s `properties` and the broker chose to answer.
  String? get responseInformation => _capabilities.responseInformation;

  /// A snapshot of the limits and options the broker announced in the most
  /// recent CONNACK.
  ///
  /// Holds the protocol defaults before the first connection. These are
  /// per-connection values, so read it again after a reconnect rather than
  /// holding on to the result. The client already enforces them; this is for
  /// diagnostics and for callers that want to adapt — batching to Receive
  /// Maximum, for example.
  ///
  /// A copy, because the client enforces its limits from the same object and
  /// a caller editing them would quietly disable those checks.
  ServerCapabilities get serverCapabilities => _capabilities.copy();

  /// Establishes (and maintains) the MQTT connection.
  ///
  /// Session state is kept in memory by this instance only; nothing is saved.
  /// Reconnects of this instance resume the broker session when
  /// [sessionExpiryInterval] is non-zero. A new instance — after a restart, or
  /// a second object in the same process — that connects with
  /// `cleanStart: false` while the broker still holds the session fails with
  /// [MqttSessionNotOwnedException] (MQTT-3.2.2-4). Connect it with
  /// `cleanStart: true`, or set [adoptBrokerSession].
  ///
  /// [adoptBrokerSession] takes the broker's session over, for an application
  /// that wants the messages queued while it was offline. The broker's state
  /// is accepted as it is, with consequences the application has to accept:
  ///
  /// * an incoming QoS 2 message whose exchange the previous holder did not
  ///   finish can be delivered a second time;
  /// * a QoS 2 publication the previous holder did not finish can be lost on
  ///   a broker that lets a new PUBLISH with the same packet identifier
  ///   replace it (mosquitto does). A broker that refuses the identifier with
  ///   0x91 instead (EMQX) gets the new publication under another one;
  /// * the session's subscriptions keep working while it lasts, but this
  ///   instance does not know them and cannot restore them after a session
  ///   loss. Subscribe again after connecting to have them restored.
  Future<void> connect({
    bool cleanStart = true,
    bool? reconnectCleanStart,
    Duration keepAlive = const Duration(seconds: 60),
    Duration? sessionExpiryInterval,
    List<MqttProperty> properties = const [],
    Duration connackTimeout = const Duration(seconds: 10),
    int receiveMaximum = 65535,
    int maximumPacketSize = 268435455,
    int topicAliasMaximum = 0,
    String? authenticationMethod,
    Uint8List? authenticationData,
    bool adoptBrokerSession = false,
  }) async {
    if (_closed) {
      throw MqttConnectionException('Client has been closed');
    }
    if (keepAlive < Duration.zero || keepAlive.inSeconds > 0xFFFF) {
      throw ArgumentError.value(
        keepAlive,
        'keepAlive',
        'Must be between zero and 65535 seconds (18h12m15s)',
      );
    }
    if (receiveMaximum < 1 || receiveMaximum > 0xFFFF) {
      throw ArgumentError.value(
        receiveMaximum,
        'receiveMaximum',
        'Must be between 1 and 65535',
      );
    }
    if (maximumPacketSize < 1 || maximumPacketSize > 0xFFFFFFFF) {
      throw ArgumentError.value(
        maximumPacketSize,
        'maximumPacketSize',
        'Must be between 1 and 4294967295',
      );
    }
    if (topicAliasMaximum < 0 || topicAliasMaximum > 0xFFFF) {
      throw ArgumentError.value(
        topicAliasMaximum,
        'topicAliasMaximum',
        'Must be between 0 and 65535',
      );
    }
    final expiry = sessionExpiryInterval;
    if (expiry != null &&
        (expiry < Duration.zero || expiry.inSeconds > 0xFFFFFFFF)) {
      throw ArgumentError.value(
        expiry,
        'sessionExpiryInterval',
        'Must be between zero and 4294967295 seconds',
      );
    }
    if (connackTimeout <= Duration.zero) {
      throw ArgumentError.value(
        connackTimeout,
        'connackTimeout',
        'Must be greater than zero',
      );
    }
    if (authenticationData != null && authenticationMethod == null) {
      throw ArgumentError.value(
        authenticationData,
        'authenticationData',
        'Requires authenticationMethod',
      );
    }
    if (authenticationData != null && authenticationData.length > 0xFFFF) {
      throw ArgumentError.value(
        authenticationData,
        'authenticationData',
        'Must not exceed 65535 bytes',
      );
    }
    _rejectDuplicateConnectProperties(
      properties,
      sessionExpiryInterval: sessionExpiryInterval,
      receiveMaximum: receiveMaximum,
      maximumPacketSize: maximumPacketSize,
      topicAliasMaximum: topicAliasMaximum,
      authenticationMethod: authenticationMethod,
      authenticationData: authenticationData,
    );
    // Brokers disagree about an empty client identifier with clean start
    // false (one assigns an id, another rejects 0x85). The packet is not
    // sent. A broker-assigned id from an earlier clean start is kept in
    // [effectiveClientId] and may resume.
    if (effectiveClientId.isEmpty && !cleanStart) {
      throw ArgumentError(
        'An empty clientId requires cleanStart: true',
      );
    }
    // Arguments are validated even when the client is already connected, so a
    // caller that passes a bad value is told rather than silently ignored.
    if (state == MqttConnectionState.disconnecting) {
      throw StateError(
        'wait for disconnect() to finish before calling connect()',
      );
    }
    if (state == MqttConnectionState.connected ||
        state == MqttConnectionState.connecting ||
        state == MqttConnectionState.reconnecting ||
        state == MqttConnectionState.authenticating) {
      if (!_sameConnectSettings(
        cleanStart: cleanStart,
        reconnectCleanStart: reconnectCleanStart,
        keepAlive: keepAlive,
        sessionExpiryInterval: sessionExpiryInterval,
        properties: properties,
        connackTimeout: connackTimeout,
        receiveMaximum: receiveMaximum,
        maximumPacketSize: maximumPacketSize,
        topicAliasMaximum: topicAliasMaximum,
        authenticationMethod: authenticationMethod,
        authenticationData: authenticationData,
        adoptBrokerSession: adoptBrokerSession,
      )) {
        throw StateError('disconnect() before changing connection settings');
      }
      // The session and the negotiated settings belong to the live connection.
      // Re-applying them here would discard in-flight state without any packet
      // reaching the broker. Join the existing attempt.
      await _connectionManager.start(_buildConnectPacket);
      return;
    }
    _cleanStart = cleanStart;
    _reconnectCleanStart = reconnectCleanStart;
    _hasConnectedOnce = false;
    _keepAlive = keepAlive;
    _sessionExpiryInterval = sessionExpiryInterval;
    _connectProperties = properties;
    _connackTimeout = connackTimeout;
    _clientReceiveMaximum = receiveMaximum;
    _clientMaximumPacketSize = maximumPacketSize;
    _clientTopicAliasMaximum = topicAliasMaximum;
    _authenticationMethod = authenticationMethod;
    _authenticationData = authenticationData;
    _adoptBrokerSession = adoptBrokerSession;
    _connectionManager.connackTimeout = connackTimeout;
    _connectionManager.clientMaximumPacketSize = maximumPacketSize;
    _connectionManager.authenticator = authenticator;

    if (!cleanStart &&
        _requestedExpirySeconds(sessionExpiryInterval, properties) == 0) {
      logger.log(
        MqttLogLevel.warning,
        'cleanStart is false but the session expiry interval is 0, so the '
        'broker ends the session when the connection closes. Set '
        'sessionExpiryInterval to keep it.',
      );
    }

    if (cleanStart) {
      _ownsSession = false;
      _assignedClientId = null;
      _discardSession(notify: false, clearSubscriptions: true);
    }

    // Passing a builder rather than a packet so every reconnect picks up the
    // broker-assigned client identifier and the current session settings.
    await _connectionManager.start(_buildConnectPacket);
  }

  /// Re-authenticates the live connection (specification section 4.12.1).
  ///
  /// Only available when [connect] supplied an `authenticationMethod`
  /// (MQTT-4.12.0-7) and an [authenticator] is configured. The same method is
  /// reused; [authenticationData] carries the first round of data when the
  /// mechanism expects the client to speak first.
  ///
  /// Publishing and subscribing continue to work while this is in flight, as
  /// section 4.12.1 requires. If the broker rejects the exchange the
  /// connection is closed and this throws.
  Future<void> reauthenticate({Uint8List? authenticationData}) {
    _ensureConnected();
    if (authenticationData != null && authenticationData.length > 0xFFFF) {
      throw ArgumentError.value(
        authenticationData,
        'authenticationData',
        'Must not exceed 65535 bytes',
      );
    }
    return _connectionManager.reauthenticate(
      authenticationData: authenticationData,
      timeout: operationTimeout <= Duration.zero ? null : operationTimeout,
    );
  }

  /// Sends a DISCONNECT packet and closes the connection.
  ///
  /// Subscribe and unsubscribe calls still awaiting an acknowledgement, and
  /// publications still waiting for a send slot, fail with
  /// [MqttConnectionException].
  ///
  /// QoS 1/2 publications already handed to the session are kept when the
  /// session outlives the connection (a non-zero Session Expiry Interval, not
  /// lowered to zero by [properties]): a later `connect(cleanStart: false)`
  /// re-sends them, as section 4.4 requires, and their futures complete then
  /// (or fail if the session is lost or [close] is called). Without such a
  /// session they fail now.
  ///
  /// [reasonCode] must be one Table 3-10 allows a client to send.
  Future<void> disconnect({
    MqttReasonCode reasonCode = MqttReasonCode.success,
    List<MqttProperty> properties = const [],
  }) async {
    _validateDisconnectReason(reasonCode);
    _validateDisconnectProperties(properties);
    final keepSession = _sessionOutlives(properties);
    _stopAckWatch();
    _connected = false;
    _hasConnectedOnce = false;
    _connectionManager.beginDisconnect();
    try {
      _connectionManager.send(
        MqttDisconnectPacket(reasonCode: reasonCode, properties: properties),
      );
    } on Object catch (e) {
      logger.log(MqttLogLevel.debug, 'DISCONNECT send failed: $e');
    }
    // stop() closes the transport, which flushes the queued DISCONNECT.
    await _connectionManager.stop();
    final error =
        MqttConnectionException('Disconnected before acknowledgement');
    _flow.failWaiters(error);
    _flow.suspend();
    _resumeQueue.clear();
    _failPendingSubscribes(error);
    _failPendingUnsubscribes(error);
    if (!keepSession) {
      _ownsSession = false;
      _abortPending(error);
    }
  }

  /// Disconnects and releases every resource held by the client.
  ///
  /// The client cannot be reconnected afterwards; [messages], [stateStream]
  /// and [errors] are closed, and every pending operation fails.
  Future<void> close({
    MqttReasonCode reasonCode = MqttReasonCode.success,
  }) async {
    if (_closed) {
      return;
    }
    _validateDisconnectReason(reasonCode);
    _closed = true;
    await disconnect(reasonCode: reasonCode);
    _abortPending(MqttConnectionException('Client closed'));
    _undelivered.clear();
    _undeliveredQos0 = 0;
    await _connectionManager.dispose();
    await _messages.close();
    await _errors.close();
    await _errorEvents.close();
  }

  /// DISCONNECT reason codes a client may send (Table 3-10).
  static const Set<int> _clientDisconnectReasonCodes = {
    0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x90, //
    0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99,
  };

  static void _validateDisconnectReason(MqttReasonCode reasonCode) {
    if (!_clientDisconnectReasonCodes.contains(reasonCode.value)) {
      throw ArgumentError.value(
        reasonCode,
        'reasonCode',
        'Only the server may send this DISCONNECT reason code',
      );
    }
  }

  /// Whether the broker keeps the session after a DISCONNECT carrying
  /// [properties]: the Session Expiry Interval in force is the DISCONNECT's,
  /// else the one the broker granted in CONNACK, else the one requested.
  bool _sessionOutlives(List<MqttProperty> properties) {
    for (final property in properties) {
      if (property is SessionExpiryInterval) {
        return property.seconds > 0;
      }
    }
    final granted = _capabilities.sessionExpiryInterval;
    if (granted != null) {
      return granted > Duration.zero;
    }
    return _requestedSessionExpirySeconds() > 0;
  }

  /// Rejects a CONNECT property that the named [connect] arguments will also
  /// emit.
  ///
  /// Either spelling on its own is supported: [_buildConnectPacket] only emits
  /// one of these properties when its argument differs from the protocol
  /// default, so a caller may pass the property directly instead. Supplying
  /// both is the case that has to be caught. None of them may repeat in a
  /// property section, so it would otherwise fail deep inside the encoder as a
  /// wire-level "duplicate property" — reported as a protocol error,
  /// classified as not retryable, and ending the connection loop over what is
  /// really a mistake at the call site.
  static void _rejectDuplicateConnectProperties(
    List<MqttProperty> properties, {
    required Duration? sessionExpiryInterval,
    required int receiveMaximum,
    required int maximumPacketSize,
    required int topicAliasMaximum,
    required String? authenticationMethod,
    required Uint8List? authenticationData,
  }) {
    for (final property in properties) {
      final String? argument = switch (property) {
        SessionExpiryInterval() when sessionExpiryInterval != null =>
          'sessionExpiryInterval',
        ReceiveMaximum() when receiveMaximum != 65535 => 'receiveMaximum',
        MaximumPacketSize() when maximumPacketSize != 268435455 =>
          'maximumPacketSize',
        TopicAliasMaximum() when topicAliasMaximum != 0 => 'topicAliasMaximum',
        AuthenticationMethod() when authenticationMethod != null =>
          'authenticationMethod',
        AuthenticationData() when authenticationData != null =>
          'authenticationData',
        _ => null,
      };
      if (argument != null) {
        throw ArgumentError.value(
          property,
          'properties',
          '${property.propertyName} is already set by the $argument argument; '
              'supply it one way or the other, not both',
        );
      }
    }
  }

  /// Rejects a DISCONNECT the broker would answer with a protocol error.
  ///
  /// Section 3.14.2.2.2: extending a session that was opened with a Session
  /// Expiry Interval of zero is a protocol error, and the broker replies with
  /// DISCONNECT 0x82 rather than honouring it.
  void _validateDisconnectProperties(List<MqttProperty> properties) {
    if (_requestedSessionExpirySeconds() > 0) {
      return;
    }
    for (final property in properties) {
      if (property is SessionExpiryInterval && property.seconds != 0) {
        throw ArgumentError.value(
          property.seconds,
          'properties',
          'A non-zero Session Expiry Interval cannot be set on DISCONNECT '
              'when CONNECT requested zero',
        );
      }
    }
  }

  /// The Session Expiry Interval the last CONNECT actually carried.
  ///
  /// It can arrive either as the [connect] parameter or inside its
  /// `properties`, and the DISCONNECT rule is about what went on the wire.
  int _requestedSessionExpirySeconds() =>
      _requestedExpirySeconds(_sessionExpiryInterval, _connectProperties);

  /// Fails every operation waiting for a broker acknowledgement and reclaims
  /// the session slots they held.
  ///
  /// A QoS 1/2 publication stays in the session store until the broker
  /// acknowledges it, so nothing but an explicit teardown or a session
  /// discard releases it. This is that teardown: without it a broker that
  /// silently stops acknowledging would grow the store and the identifier
  /// pool without bound.
  void _abortPending(Object error) {
    _failPendingSubscribes(error);
    _failPendingUnsubscribes(error);
    for (final entry in _session.outgoingQos1.entries.toList()) {
      if (!entry.completer.isCompleted) {
        entry.completer.completeError(error);
      }
      _session.packetIds.release(entry.packetIdentifier);
    }
    for (final entry in _session.outgoingQos2.entries.toList()) {
      if (!entry.completer.isCompleted) {
        entry.completer.completeError(error);
      }
      _session.packetIds.release(entry.packetIdentifier);
    }
    _session.outgoingQos1.clear();
    _session.outgoingQos2.clear();
    _resumeQueue.clear();
    _flow.failWaiters(error);
    _flow.suspend();
  }

  void _onFatalError(Object error, StackTrace stackTrace) {
    _stopAckWatch();
    _connected = false;
    _abortPending(error);
    _emitReportedError(error, stackTrace);
  }

  /// Reports [error] on [errorEvents] and [errors]. A listener that throws is
  /// logged once and does not re-enter this method.
  void _reportListenerError(Object error, StackTrace stackTrace) {
    if (_reportingListenerError) {
      logger.log(
        MqttLogLevel.error,
        'Listener error while reporting an error: $error',
      );
      return;
    }
    _reportingListenerError = true;
    try {
      _emitReportedError(error, stackTrace);
    } finally {
      _reportingListenerError = false;
    }
  }

  void _emitReportedError(Object error, StackTrace stackTrace) {
    if (!_errorEvents.isClosed) {
      _errorEvents.add(MqttErrorEvent(error, stackTrace));
    }
    if (_errors.isClosed) {
      logger.log(
        MqttLogLevel.error,
        'Unhandled connection error (errors stream closed): $error',
      );
      return;
    }
    if (_errors.hasListener) {
      _errors.add(error);
    } else {
      logger.log(
        MqttLogLevel.error,
        'Unhandled connection error (no errors listener): $error',
      );
    }
  }

  /// Subscribes to [topicFilter], completing when the broker acknowledges.
  ///
  /// Throws [MqttServerRejectedException] if the broker rejects any filter.
  Future<void> subscribe(
    String topicFilter, {
    MqttSubscriptionOptions options = const MqttSubscriptionOptions(),
    int? subscriptionIdentifier,
  }) {
    return subscribeAll(
      [MqttSubscription(topicFilter, options: options)],
      subscriptionIdentifier: subscriptionIdentifier,
    );
  }

  /// Subscribes to multiple topic filters in a single SUBSCRIBE packet.
  ///
  /// If the broker accepts some filters and rejects others, the accepted ones
  /// are recorded (and will be re-established after a session loss) before
  /// [MqttServerRejectedException] is thrown for the rejected ones.
  Future<void> subscribeAll(
    List<MqttSubscription> subscriptions, {
    int? subscriptionIdentifier,
  }) async {
    _ensureConnected();
    if (subscriptions.isEmpty) {
      return;
    }
    for (final subscription in subscriptions) {
      _validateSubscription(subscription);
    }
    if (subscriptionIdentifier != null) {
      if (!_capabilities.subscriptionIdentifierAvailable) {
        throw MqttFlowControlException(
          'Subscription identifiers are not supported by the server',
        );
      }
      if (subscriptionIdentifier < 1 ||
          subscriptionIdentifier > VariableByteInteger.maxValue) {
        throw ArgumentError.value(
          subscriptionIdentifier,
          'subscriptionIdentifier',
          'Must be between 1 and ${VariableByteInteger.maxValue}',
        );
      }
    }
    final deadline = _deadline();
    final packetIdentifier =
        await _session.packetIds.allocate(deadline: deadline);
    final completer = Completer<MqttSubackPacket>();
    _pendingSubscribes[packetIdentifier] = _PendingSubscribe(
      completer: completer,
      subscriptions: subscriptions,
      subscriptionIdentifier: subscriptionIdentifier,
    );
    var keepInflight = false;
    try {
      _connectionManager.send(
        MqttSubscribePacket(
          packetIdentifier: packetIdentifier,
          subscriptions: subscriptions,
          properties: [
            if (subscriptionIdentifier != null)
              SubscriptionIdentifier(subscriptionIdentifier),
          ],
        ),
      );
      final suback = await _awaitAck(completer.future, 'SUBACK', deadline);
      // A reason-code count mismatch is a protocol error. The packet handler
      // completes this future with it and disconnects; checking again here
      // would run only on the success path.
      _throwIfSubackRejected(suback);
    } on MqttTimeoutException {
      // A timeout is this client giving up on the answer, not the broker
      // confirming it will never send one. The identifier only becomes
      // reusable once its SUBACK arrives (MQTT-2.2.1-4), so it stays reserved
      // until a late SUBACK retires it or the connection ends and
      // [_failPendingSubscribes] reclaims everything still outstanding.
      keepInflight = true;
      rethrow;
    } finally {
      if (!keepInflight) {
        _retirePendingSubscribe(packetIdentifier);
      }
    }
  }

  void _retirePendingSubscribe(int packetIdentifier) {
    if (_pendingSubscribes.remove(packetIdentifier) != null) {
      _session.packetIds.release(packetIdentifier);
    }
  }

  /// Drops a pending operation and returns its packet identifier to the pool.
  ///
  /// The identifier is released only when this call is the one that removed
  /// the entry. The acknowledgement handler may have retired it already, and
  /// releasing twice can hand an identifier back while a second operation is
  /// using it (MQTT-2.2.1-4).
  void _retirePending<T>(Map<int, Completer<T>> pending, int packetIdentifier) {
    if (pending.remove(packetIdentifier) != null) {
      _session.packetIds.release(packetIdentifier);
    }
  }

  /// The instant an operation started now must finish by, or null when
  /// [operationTimeout] is disabled.
  ///
  /// One deadline covers waiting for a packet identifier, waiting for a
  /// Receive Maximum slot, and — for subscribe and unsubscribe — waiting for
  /// the acknowledgement. A QoS 1/2 publish that has entered the session
  /// store waits for its acknowledgement without this deadline.
  DateTime? _deadline() => operationTimeout <= Duration.zero
      ? null
      : DateTime.now().add(operationTimeout);

  /// Waits for a broker acknowledgement, applying [operationTimeout].
  Future<T> _awaitAck<T>(Future<T> future, String what, [DateTime? deadline]) {
    if (operationTimeout <= Duration.zero) {
      return future;
    }
    final remaining = (deadline ?? DateTime.now().add(operationTimeout))
        .difference(DateTime.now());
    return future.timeout(
      remaining <= Duration.zero ? Duration.zero : remaining,
      onTimeout: () => throw MqttTimeoutException(
        'Timed out after ${operationTimeout.inSeconds}s waiting for $what',
      ),
    );
  }

  void _validateSubscription(MqttSubscription subscription) {
    final topicFilter = subscription.topicFilter;
    _validateTopicFilter(topicFilter);
    final shared = MqttTopic.isShared(topicFilter);
    if (shared && !_capabilities.sharedSubscriptionAvailable) {
      throw MqttFlowControlException(
        'Shared subscriptions are not supported by the server',
      );
    }
    // MQTT-3.8.3-4: No Local must not be set on a Shared Subscription.
    if (shared && subscription.options.noLocal) {
      throw ArgumentError.value(
        topicFilter,
        'topicFilter',
        'No Local must not be set on a shared subscription',
      );
    }
    // A ShareName cannot hold a wildcard, so only the filter part matters.
    final effective = MqttTopic.sharedFilterOf(topicFilter);
    if ((effective.contains('+') || effective.contains('#')) &&
        !_capabilities.wildcardSubscriptionAvailable) {
      throw MqttFlowControlException(
        'Wildcard subscriptions are not supported by the server',
      );
    }
  }

  /// Rejects topic filters the broker is required to reject (section 4.7.1
  /// and 4.8.2), for both SUBSCRIBE and UNSUBSCRIBE.
  void _validateTopicFilter(String topicFilter) {
    final problem = MqttTopic.checkFilter(topicFilter);
    if (problem != null) {
      throw ArgumentError.value(
        topicFilter,
        'topicFilter',
        'A topic filter $problem',
      );
    }
  }

  /// Unsubscribes from [topicFilters], completing when the broker acknowledges.
  Future<void> unsubscribe(List<String> topicFilters) async {
    _ensureConnected();
    if (topicFilters.isEmpty) {
      return;
    }
    for (final topicFilter in topicFilters) {
      _validateTopicFilter(topicFilter);
    }
    final deadline = _deadline();
    final packetIdentifier =
        await _session.packetIds.allocate(deadline: deadline);
    final completer = Completer<MqttUnsubackPacket>();
    _pendingUnsubscribes[packetIdentifier] = completer;
    _unsubscribeCounts[packetIdentifier] = topicFilters.length;
    var keepInflight = false;
    try {
      _connectionManager.send(
        MqttUnsubscribePacket(
          packetIdentifier: packetIdentifier,
          topicFilters: topicFilters,
        ),
      );
      final unsuback = await _awaitAck(completer.future, 'UNSUBACK', deadline);
      for (var i = 0; i < topicFilters.length; i++) {
        if (unsuback.reasonCodes[i] < 0x80) {
          _session.subscriptions.remove(topicFilters[i]);
        }
      }
      for (final reasonCode in unsuback.reasonCodes) {
        if (reasonCode >= 0x80) {
          throw MqttServerRejectedException(
            reasonCode,
            'Unsubscribe failed',
          );
        }
      }
    } on MqttTimeoutException {
      // See the matching note in [subscribeAll]: the identifier stays reserved
      // until the UNSUBACK arrives or the connection ends.
      keepInflight = true;
      rethrow;
    } finally {
      if (!keepInflight) {
        _unsubscribeCounts.remove(packetIdentifier);
        _retirePending(_pendingUnsubscribes, packetIdentifier);
      }
    }
  }

  /// Publishes [payload] to [topic].
  ///
  /// For QoS 0 the future completes once the packet is written. For QoS 1 and
  /// QoS 2 it completes once the broker's acknowledgement completes the
  /// protocol exchange. Publications are put on the wire in the order of the
  /// calls (per QoS level), also when they have to wait for the server's
  /// Receive Maximum. A QoS 1/2 publication accepted into the session survives
  /// a lost connection: its future completes on PUBACK/PUBCOMP (or an error
  /// PUBREC), a session loss, a [disconnect] that ends the session, or
  /// [close]. [operationTimeout] bounds only the wait for a send slot or a
  /// packet identifier, not that acknowledgement.
  ///
  /// A failure reported by the broker (for example 0x87 Not authorized) is a
  /// result, not an exception: check [MqttPublishResult.isError]. Reason code
  /// 0x10 (no matching subscribers) is a success.
  Future<MqttPublishResult> publish(
    String topic,
    Uint8List payload, {
    MqttQos qos = MqttQos.atMostOnce,
    bool retain = false,
    List<MqttProperty> properties = const [],
  }) async {
    _ensureConnected();
    _validatePublishTopic(topic);
    for (final property in properties) {
      // MQTT-3.3.4-6: only the server attaches Subscription Identifiers.
      if (property is SubscriptionIdentifier) {
        throw ArgumentError.value(
          property,
          'properties',
          'A client must not send a Subscription Identifier in PUBLISH',
        );
      }
      // Section 3.3.2.3.4: a Topic Alias of 0 is a protocol error. Reject it
      // before the encoder turns it into a disconnect.
      if (property is TopicAlias && property.value == 0) {
        throw ArgumentError.value(
          property.value,
          'properties',
          'Topic Alias must not be 0',
        );
      }
    }
    if (qos.value > _capabilities.maximumQos) {
      throw MqttFlowControlException(
        'The server only supports maximum QoS ${_capabilities.maximumQos}',
      );
    }
    if (retain && !_capabilities.retainAvailable) {
      throw MqttFlowControlException(
        'The server does not support retained messages',
      );
    }
    metrics.messagesPublished++;
    switch (qos) {
      case MqttQos.atMostOnce:
        final aliased = _applyOutgoingAlias(topic, properties);
        _connectionManager.send(
          MqttPublishPacket(
            topicName: aliased.topic,
            payload: payload,
            qos: qos,
            retain: retain,
            properties: aliased.properties,
          ),
        );
        aliased.commit();
        return const MqttPublishResult();
      case MqttQos.atLeastOnce:
      case MqttQos.exactlyOnce:
        return _publishWithAck(qos, topic, payload,
            retain: retain, properties: properties);
    }
  }

  /// Rejects topic names that the broker is required to reject, so the caller
  /// gets a local error instead of being disconnected.
  void _validatePublishTopic(String topic) {
    final problem = MqttTopic.checkName(topic);
    if (problem != null) {
      throw ArgumentError.value(
          topic, 'topic', 'A publish topic name $problem');
    }
  }

  /// Applies the client-to-server Topic Alias mapping to an outgoing publish.
  ///
  /// Returns the possibly-aliased topic name and the properties to send. A
  /// newly reserved alias is only bound to the topic once [_Aliased.commit] is
  /// called, which must happen after the PUBLISH carrying the full topic name
  /// has been written: an alias the broker never received would make every
  /// later publish reference an unknown alias.
  ///
  /// Stored session entries always keep the original topic/properties so a
  /// retransmit after reconnect uses the full topic name.
  _Aliased _applyOutgoingAlias(String topic, List<MqttProperty> properties) {
    TopicAlias? supplied;
    for (final property in properties) {
      if (property is TopicAlias) {
        supplied = property;
        break;
      }
    }
    if (supplied != null) {
      // MQTT-3.3.2-9: a client must not send a Topic Alias greater than the
      // Topic Alias Maximum the server returned in CONNACK. The broker answers
      // one by closing the connection, so fail the call instead.
      if (supplied.value > _capabilities.topicAliasMaximum) {
        throw MqttFlowControlException(
          'Topic alias ${supplied.value} exceeds the server maximum of '
          '${_capabilities.topicAliasMaximum}',
        );
      }
      // The caller is establishing the mapping by hand, so record it once the
      // packet carrying the full topic name has been written. Without this the
      // binding is invisible to the map: every later publish to the same topic
      // would send the full name again, and [reserve] could hand the very same
      // alias to a different topic.
      return _Aliased(
        topic,
        properties,
        (alias: supplied.value, topic: topic),
        _outgoingAliases,
      );
    }
    final existing = _outgoingAliases.aliasFor(topic);
    if (existing != null) {
      return _Aliased(
        '',
        [...properties, TopicAlias(existing)],
        null,
        _outgoingAliases,
      );
    }
    final reserved = _outgoingAliases.reserve();
    if (reserved != null) {
      return _Aliased(
        topic,
        [...properties, TopicAlias(reserved)],
        (alias: reserved, topic: topic),
        _outgoingAliases,
      );
    }
    return _Aliased(topic, properties, null, _outgoingAliases);
  }

  /// The QoS 1/2 publish path.
  ///
  /// The send slot is taken first and in call order (see [FlowController]);
  /// the packet identifier only afterwards, so an identifier is never held
  /// across a reconnect by a publication that is still waiting, and a session
  /// reset cannot hand the same identifier out twice.
  Future<MqttPublishResult> _publishWithAck(
    MqttQos qos,
    String topic,
    Uint8List payload, {
    required bool retain,
    required List<MqttProperty> properties,
  }) async {
    final deadline = _deadline();
    await _flow.acquire(deadline: deadline);
    final int packetIdentifier;
    try {
      packetIdentifier = await _session.packetIds.allocate(deadline: deadline);
    } on Object {
      _releaseFlow();
      rethrow;
    }
    final sequence = _session.nextSequence();
    final Completer<MqttPublishResult> completer;
    final AwaitingAck tracked;
    if (qos == MqttQos.atLeastOnce) {
      final entry = OutgoingQos1Entry(
        packetIdentifier: packetIdentifier,
        sequence: sequence,
        topic: topic,
        payload: payload,
        retain: retain,
        properties: properties,
      );
      _session.outgoingQos1.put(entry);
      completer = entry.completer;
      tracked = entry;
    } else {
      final entry = OutgoingQos2Entry(
        packetIdentifier: packetIdentifier,
        sequence: sequence,
        topic: topic,
        payload: payload,
        retain: retain,
        properties: properties,
      );
      _session.outgoingQos2.put(entry);
      completer = entry.completer;
      tracked = entry;
    }
    try {
      final aliased = _applyOutgoingAlias(topic, properties);
      _connectionManager.send(
        MqttPublishPacket(
          topicName: aliased.topic,
          payload: payload,
          qos: qos,
          retain: retain,
          packetIdentifier: packetIdentifier,
          properties: aliased.properties,
        ),
      );
      aliased.commit();
      _markSent(tracked);
    } on MqttConnectionException {
      // The connection dropped between taking the slot and writing. The
      // publication is in the session store, exactly as if it had been
      // written and lost: the next resume sends it, a lost session fails it.
    } on Object {
      // Never reached the wire and never will (too large, unencodable).
      _session.outgoingQos1.remove(packetIdentifier);
      _session.outgoingQos2.remove(packetIdentifier);
      _session.packetIds.release(packetIdentifier);
      _releaseFlow();
      rethrow;
    }
    // In the session store the publication owns its packet identifier and its
    // Receive Maximum slot until the broker acknowledges it (section 4.4,
    // section 4.9). Timing the future out would release the slot while the
    // broker still holds the message, and the caller's retry would duplicate
    // it. [operationTimeout] already bounded the wait to get here;
    // [ackTimeout] bounds the wait for the acknowledgement by replacing a
    // connection that stops answering.
    return completer.future;
  }

  /// Re-sends [entry]'s publication under a fresh packet identifier after the
  /// broker refused the first transmission with 0x91 (Packet Identifier in
  /// use). A 0x91 PUBACK/PUBREC is an error acknowledgement: the broker did
  /// not take the message, so a retry cannot duplicate it.
  ///
  /// This happens when the broker still holds state for that identifier from
  /// an earlier holder of the session, for example a previous process that
  /// connected with the same client identifier. The old identifier is left
  /// reserved for the rest of the session so it is not handed out again.
  void _republish(Object entry, int oldIdentifier) {
    final newIdentifier = _session.packetIds.tryAllocate();
    if (newIdentifier == null) {
      _releaseFlow();
      final completer = entry is OutgoingQos1Entry
          ? entry.completer
          : (entry as OutgoingQos2Entry).completer;
      completer.complete(
        const MqttPublishResult(
            reasonCode: MqttReasonCode.packetIdentifierInUse),
      );
      return;
    }
    logger.log(
      MqttLogLevel.warning,
      'Broker reports packet identifier $oldIdentifier in use; re-publishing '
      'as $newIdentifier',
    );
    final OutgoingQos1Entry? q1 =
        entry is OutgoingQos1Entry ? entry.withIdentifier(newIdentifier) : null;
    final OutgoingQos2Entry? q2 =
        entry is OutgoingQos2Entry ? entry.withIdentifier(newIdentifier) : null;
    final AwaitingAck tracked;
    if (q1 != null) {
      _session.outgoingQos1.put(q1);
      tracked = q1;
    } else {
      _session.outgoingQos2.put(q2!);
      tracked = q2;
    }
    try {
      _connectionManager.send(
        MqttPublishPacket(
          topicName: q1?.topic ?? q2!.topic,
          payload: q1?.payload ?? q2!.payload,
          qos: q1 != null ? MqttQos.atLeastOnce : MqttQos.exactlyOnce,
          retain: q1?.retain ?? q2!.retain,
          packetIdentifier: newIdentifier,
          properties: _withoutTopicAlias(q1?.properties ?? q2!.properties),
        ),
      );
    } on MqttConnectionException {
      // The connection is going away. The new entry is in the session store
      // like any other unsent publication: a resume sends it.
      return;
    } on MqttPacketTooLargeException catch (error, stackTrace) {
      // Without its Topic Alias the PUBLISH can exceed the server's Maximum
      // Packet Size. It never reached the broker, so fail it here.
      _session.outgoingQos1.remove(newIdentifier);
      _session.outgoingQos2.remove(newIdentifier);
      _session.packetIds.release(newIdentifier);
      _releaseFlow();
      (q1?.completer ?? q2!.completer).completeError(error, stackTrace);
      return;
    }
    _markSent(tracked);
  }

  void _throwIfSubackRejected(MqttSubackPacket suback) {
    for (final reasonCode in suback.reasonCodes) {
      if (reasonCode >= 0x80) {
        throw MqttServerRejectedException(
          reasonCode,
          'Subscribe failed with reason code $reasonCode',
        );
      }
    }
  }

  void _onPacket(MqttPacket packet) {
    switch (packet) {
      case MqttPublishPacket publish:
        _handlePublish(publish);
      case MqttSubackPacket suback:
        _handleSuback(suback);
      case MqttUnsubackPacket unsuback:
        _handleUnsuback(unsuback);
      case MqttPubackPacket puback:
        _handlePuback(puback);
      case MqttPubrecPacket pubrec:
        _handlePubrec(pubrec);
      case MqttPubrelPacket pubrel:
        _handlePubrel(pubrel);
      case MqttPubcompPacket pubcomp:
        _handlePubcomp(pubcomp);
      case MqttDisconnectPacket disconnect:
        _handleServerDisconnect(disconnect);
      default:
        break;
    }
  }

  void _handleSuback(MqttSubackPacket suback) {
    final pending = _pendingSubscribes.remove(suback.packetIdentifier);
    if (pending == null) {
      logger.log(
        MqttLogLevel.debug,
        'Ignoring SUBACK for unknown packet identifier ${suback.packetIdentifier}',
      );
      return;
    }
    _session.packetIds.release(suback.packetIdentifier);
    final subscriptions = pending.subscriptions;
    if (suback.reasonCodes.length != subscriptions.length) {
      final error = MqttProtocolException(
        'SUBACK carries ${suback.reasonCodes.length} reason code(s) for '
        '${subscriptions.length} topic filter(s)',
      );
      if (!pending.completer.isCompleted) {
        pending.completer.completeError(error);
      }
      throw error;
    }
    for (var i = 0; i < subscriptions.length; i++) {
      if (suback.reasonCodes[i] < 0x80) {
        _session.subscriptions.add(
          subscriptions[i],
          subscriptionIdentifier: pending.subscriptionIdentifier,
        );
      }
    }
    if (!pending.completer.isCompleted) {
      pending.completer.complete(suback);
    }
  }

  void _handleUnsuback(MqttUnsubackPacket unsuback) {
    final packetIdentifier = unsuback.packetIdentifier;
    final completer = _pendingUnsubscribes.remove(packetIdentifier);
    final expected = _unsubscribeCounts.remove(packetIdentifier);
    if (completer == null) {
      logger.log(
        MqttLogLevel.debug,
        'Ignoring acknowledgement for unknown packet identifier '
        '$packetIdentifier',
      );
      return;
    }
    _session.packetIds.release(packetIdentifier);
    if (expected != null && unsuback.reasonCodes.length != expected) {
      final error = MqttProtocolException(
        'UNSUBACK carries ${unsuback.reasonCodes.length} reason code(s) for '
        '$expected topic filter(s)',
      );
      if (!completer.isCompleted) {
        completer.completeError(error);
      }
      throw error;
    }
    if (!completer.isCompleted) {
      completer.complete(unsuback);
    }
  }

  /// Handles a DISCONNECT sent by the broker.
  ///
  /// The broker discards anything sent after it, so the connection is torn
  /// down here rather than waiting for the peer to close the socket.
  void _handleServerDisconnect(MqttDisconnectPacket disconnect) {
    final reasonCode = disconnect.reasonCode;
    String? serverReference;
    for (final property in disconnect.properties) {
      if (property is ServerReference) {
        serverReference = property.value;
      }
    }
    if (reasonCode == MqttReasonCode.useAnotherServer ||
        reasonCode == MqttReasonCode.serverMoved) {
      onServerMoved?.call(serverReference, reasonCode!);
    }
    _connected = false;
    unawaited(_connectionManager.handleServerDisconnect(disconnect));
  }

  void _handlePublish(MqttPublishPacket publish) {
    final topic = _resolveIncomingTopic(publish);
    final id = publish.packetIdentifier;
    final epoch = _epoch;
    switch (publish.qos) {
      case MqttQos.atMostOnce:
        _deliver(publish, topic, null);
      case MqttQos.atLeastOnce:
        _checkReceiveMaximum();
        _pendingQos1Acks.add(id);
        _deliver(publish, topic, () {
          if (epoch == _epoch) {
            _pendingQos1Acks.remove(id);
            _sendAcknowledgement(MqttPubackPacket(packetIdentifier: id));
          }
        });
      case MqttQos.exactlyOnce:
        // A PUBLISH re-sent on this connection for an exchange begun on an
        // earlier one consumes this connection's quota like any other.
        if (!_session.incomingQos2.isCurrent(id, epoch)) {
          _checkReceiveMaximum();
        }
        if (_session.incomingQos2.add(id, epoch)) {
          _deliver(publish, topic, () {
            if (epoch == _epoch && _session.incomingQos2.contains(id)) {
              _sendAcknowledgement(MqttPubrecPacket(packetIdentifier: id));
            }
          });
        } else {
          // A duplicate of an exchange in progress: the message is already
          // delivered or waiting for a listener, so it is acknowledged now
          // and never handed out twice (section 4.3.3, method B).
          _sendAcknowledgement(MqttPubrecPacket(packetIdentifier: id));
        }
    }
  }

  /// Section 4.9: the broker may not have more unacknowledged QoS 1/2
  /// publications outstanding than the Receive Maximum this client declared.
  void _checkReceiveMaximum() {
    final unacknowledged =
        _pendingQos1Acks.length + _session.incomingQos2.countIn(_epoch);
    if (unacknowledged >= _clientReceiveMaximum) {
      throw MqttReceiveMaximumExceededException(
        'Broker sent more than the declared Receive Maximum of '
        '$_clientReceiveMaximum unacknowledged QoS 1/QoS 2 publications',
      );
    }
  }

  /// Sends an acknowledgement if there is a connection to send it on. When
  /// there is none, the broker re-sends the publication after reconnecting
  /// and that copy is acknowledged instead.
  void _sendAcknowledgement(MqttPacket packet) {
    try {
      _connectionManager.send(packet);
    } on MqttConnectionException {
      logger.log(MqttLogLevel.debug,
          'Not connected; ${packet.type.name} left for the broker to re-send');
    }
  }

  /// Resolves the effective topic name of an incoming PUBLISH, applying the
  /// server-to-client Topic Alias mapping.
  String _resolveIncomingTopic(MqttPublishPacket publish) {
    TopicAlias? aliasProperty;
    for (final property in publish.properties) {
      if (property is TopicAlias) {
        aliasProperty = property;
        break;
      }
    }
    if (aliasProperty == null) {
      if (publish.topicName.isEmpty) {
        throw MqttProtocolException(
          'PUBLISH with an empty topic name and no topic alias',
        );
      }
      return publish.topicName;
    }
    final alias = aliasProperty.value;
    // Above the client's Topic Alias Maximum (and 0, which the decoder
    // already rejects) is 0x94, including when the topic name is empty.
    // An in-range alias this connection has never seen is 0x82.
    if (alias > _incomingAliases.maximum) {
      throw MqttTopicAliasInvalidException(
        'Topic alias $alias exceeds the negotiated maximum '
        '${_incomingAliases.maximum}',
      );
    }
    if (publish.topicName.isNotEmpty) {
      _incomingAliases.register(alias, publish.topicName);
      return publish.topicName;
    }
    final topic = _incomingAliases.resolve(alias);
    if (topic == null) {
      throw MqttProtocolException(
        'PUBLISH used unknown topic alias $alias',
      );
    }
    return topic;
  }

  void _handlePuback(MqttPubackPacket puback) {
    final id = puback.packetIdentifier;
    final entry = _session.outgoingQos1.remove(id);
    if (entry == null) {
      // Section 3.6.2.1 notes that a Packet Identifier the receiver does not
      // know is expected during recovery rather than an error, so an
      // acknowledgement for one is logged and dropped, not escalated.
      _unknownAcknowledgement('PUBACK', id);
      return;
    }
    final reasonCode = puback.reasonCode ?? MqttReasonCode.success;
    if (reasonCode == MqttReasonCode.packetIdentifierInUse &&
        !entry.retransmitted) {
      _republish(entry, id);
      return;
    }
    _session.packetIds.release(id);
    _releaseFlow();
    entry.completer.complete(
      MqttPublishResult(reasonCode: reasonCode, properties: puback.properties),
    );
  }

  void _handlePubrec(MqttPubrecPacket pubrec) {
    final id = pubrec.packetIdentifier;
    final entry = _session.outgoingQos2[id];
    if (entry == null) {
      _unknownAcknowledgement('PUBREC', id);
      return;
    }
    final reasonCode = pubrec.reasonCode;
    if (reasonCode != null && reasonCode.value >= 0x80) {
      if (reasonCode != MqttReasonCode.packetIdentifierInUse ||
          !entry.retransmitted) {
        _session.outgoingQos2.remove(id);
        if (reasonCode == MqttReasonCode.packetIdentifierInUse) {
          _republish(entry, id);
          return;
        }
        // MQTT-4.4.0-2: the PUBLISH counts as acknowledged and is not
        // retransmitted. Section 4.9 replenishes the quota for this case too.
        _session.packetIds.release(id);
        _releaseFlow();
        entry.completer.complete(
          MqttPublishResult(
              reasonCode: reasonCode, properties: pubrec.properties),
        );
        return;
      }
      // 0x91 to a retransmission: the broker still holds the earlier copy and
      // is waiting for its PUBREL (EMQX answers a DUP this way). Release it.
      entry.pubrelResent = true;
    }
    // A success PUBREC does not replenish the quota (section 4.9); the
    // exchange stays outstanding until PUBCOMP.
    if (entry.state == OutgoingQos2State.publishSent) {
      entry.pubrecSequence = _session.nextSequence();
      entry.state = OutgoingQos2State.pubRelSent;
    }
    _connectionManager.send(MqttPubrelPacket(packetIdentifier: id));
    _markSent(entry);
  }

  void _unknownAcknowledgement(String what, int packetIdentifier) {
    logger.log(
      MqttLogLevel.debug,
      'Ignoring $what for unknown packet identifier $packetIdentifier',
    );
  }

  void _handlePubrel(MqttPubrelPacket pubrel) {
    final known = _session.incomingQos2.contains(pubrel.packetIdentifier);
    _session.incomingQos2.remove(pubrel.packetIdentifier);
    _connectionManager.send(
      MqttPubcompPacket(
        packetIdentifier: pubrel.packetIdentifier,
        reasonCode: known ? null : MqttReasonCode.packetIdentifierNotFound,
      ),
    );
  }

  void _handlePubcomp(MqttPubcompPacket pubcomp) {
    final entry = _session.outgoingQos2.remove(pubcomp.packetIdentifier);
    if (entry == null) {
      _unknownAcknowledgement('PUBCOMP', pubcomp.packetIdentifier);
      return;
    }
    _session.packetIds.release(pubcomp.packetIdentifier);
    _releaseFlow();
    var reasonCode = pubcomp.reasonCode ?? MqttReasonCode.success;
    // Section 3.7.2.1: "not found" is not an error during recovery — the
    // broker finished the exchange before the connection broke and only the
    // PUBCOMP was lost.
    if (reasonCode == MqttReasonCode.packetIdentifierNotFound &&
        entry.pubrelResent) {
      reasonCode = MqttReasonCode.success;
    }
    entry.completer.complete(
      MqttPublishResult(reasonCode: reasonCode, properties: pubcomp.properties),
    );
  }

  /// Hands a received message to the application, or keeps it (and the
  /// acknowledgement it owes) until a listener exists.
  void _deliver(
    MqttPublishPacket publish,
    String topic,
    void Function()? acknowledge,
  ) {
    metrics.messagesReceived++;
    final message = MqttMessage(
      topic: topic,
      payload: publish.payload,
      qos: publish.qos,
      retain: publish.retain,
      duplicate: publish.dup,
      properties: publish.properties,
    );
    if (_undelivered.isEmpty && _messages.hasListener) {
      _messages.add(message);
      acknowledge?.call();
      return;
    }
    _undelivered.add((message, acknowledge));
    if (acknowledge == null && ++_undeliveredQos0 > _undeliveredQos0Limit) {
      final oldest = _undelivered.firstWhere((m) => m.$2 == null);
      _undelivered.remove(oldest);
      _undeliveredQos0--;
      logger.log(
        MqttLogLevel.warning,
        'No listener on messages: dropped the oldest of '
        '$_undeliveredQos0Limit buffered QoS 0 messages',
      );
    }
  }

  void _flushUndelivered() {
    if (_undeliveredFlushScheduled) {
      return;
    }
    _undeliveredFlushScheduled = true;
    // [Stream.first] sets its data handler after [listen] returns. Delivering
    // here would drop the backlog. A microtask runs after that assignment.
    scheduleMicrotask(() {
      _undeliveredFlushScheduled = false;
      while (_undelivered.isNotEmpty && _messages.hasListener) {
        final (message, acknowledge) = _undelivered.removeFirst();
        if (acknowledge == null) {
          _undeliveredQos0--;
        }
        _messages.add(message);
        acknowledge?.call();
      }
    });
  }

  Future<void> _onConnected(MqttConnackPacket connack) async {
    // MQTT-3.2.2-2 requires Session Present to be 0 whenever the client asked
    // for a clean start, and MQTT-3.2.2-4 requires a client with no session
    // state to close the connection if it is told one was resumed. Continuing
    // would leave the two ends disagreeing about which messages and
    // subscriptions exist.
    if (_lastSentCleanStart && connack.sessionPresent) {
      throw MqttProtocolException(
        'Broker reported Session Present after a Clean Start connection',
      );
    }
    // MQTT-3.2.2-4. Session state is not persisted, so a broker session
    // left by another client instance cannot be resumed as such. An instance
    // that already accepted a CONNACK owns its session even when nothing is
    // in flight.
    if (!_lastSentCleanStart && connack.sessionPresent && !_ownsSession) {
      if (!_adoptBrokerSession) {
        throw MqttSessionNotOwnedException(
          'Broker resumed a session this client instance holds no state for. '
          'Connect with cleanStart: true, or pass adoptBrokerSession: true to '
          'take the session over.',
        );
      }
      logger.log(
        MqttLogLevel.warning,
        'Adopting a broker session this client instance holds no state for: '
        'unfinished QoS 2 exchanges of the previous holder may repeat or be '
        'lost, and its subscriptions are not known here',
      );
    }
    _epoch++;
    _pendingQos1Acks.clear();
    _sessionPresent = connack.sessionPresent;
    _hasConnectedOnce = true;
    _applyServerCapabilities(connack);
    logger.log(
      MqttLogLevel.info,
      'Connected (sessionPresent=${connack.sessionPresent})',
    );
    if (!connack.sessionPresent) {
      // MQTT-3.2.2-5: a client that has session state and is told there is
      // none must discard it.
      _discardSession();
    }
    _flow.resume(_capabilities.receiveMaximum);
    _connectionManager.acceptIncomingPackets();
    if (connack.sessionPresent) {
      _resumeSession();
      if (_resubscribeIncomplete) {
        // The session was lost earlier and its re-subscription did not finish
        // before that connection ended. The broker kept the new, empty
        // session, so it reports one as present; send the filters again.
        _resubscribeAll(_session.subscriptions.groupedByIdentifier());
      }
    }
    _connected = true;
    // Only a CONNACK this client has accepted establishes ownership. A
    // protocol error from the packets that shared the CONNACK's read throws
    // above and leaves a fresh client without a session.
    _ownsSession = true;
    // Publications that waited for a slot go after the resumed backlog.
    _flow.dispatch();
    _startAckWatch();
  }

  void _applyServerCapabilities(MqttConnackPacket connack) {
    // Capabilities are per-connection: an absent property means "default",
    // so start from the defaults instead of keeping the previous connection's.
    _capabilities.reset();
    _connackProperties = connack.properties;
    for (final property in connack.properties) {
      switch (property) {
        case AssignedClientIdentifier assigned:
          _assignedClientId = assigned.value;
          logger.log(
            MqttLogLevel.info,
            'Broker assigned client identifier ${assigned.value}',
          );
        case ReceiveMaximum receiveMaximum:
          _capabilities.receiveMaximum = receiveMaximum.value;
        case MaximumPacketSize maximumPacketSize:
          _capabilities.maximumPacketSize = maximumPacketSize.value;
        case MaximumQos maximumQos:
          _capabilities.maximumQos = maximumQos.value;
        case RetainAvailable retainAvailable:
          _capabilities.retainAvailable = retainAvailable.value == 1;
        case TopicAliasMaximum topicAliasMaximum:
          _capabilities.topicAliasMaximum = topicAliasMaximum.value;
        case WildcardSubscriptionAvailable wildcard:
          _capabilities.wildcardSubscriptionAvailable = wildcard.value == 1;
        case SubscriptionIdentifierAvailable subId:
          _capabilities.subscriptionIdentifierAvailable = subId.value == 1;
        case SharedSubscriptionAvailable shared:
          _capabilities.sharedSubscriptionAvailable = shared.value == 1;
        case ResponseInformation responseInformation:
          _capabilities.responseInformation = responseInformation.value;
        case SessionExpiryInterval sessionExpiry:
          // Section 3.2.2.3.2: the broker may grant a shorter lease than the
          // one asked for. Reported, not adopted — the next CONNECT asks for
          // the interval the application configured, not for whatever the
          // busiest moment of this connection allowed.
          _capabilities.sessionExpiryInterval =
              Duration(seconds: sessionExpiry.seconds);
        case ServerKeepAlive serverKeepAlive:
          _capabilities.serverKeepAlive =
              Duration(seconds: serverKeepAlive.seconds);
        default:
          break;
      }
    }
    _connectionManager.maximumPacketSize = _capabilities.maximumPacketSize;
    _outgoingAliases.maximum = _capabilities.topicAliasMaximum;
    _incomingAliases.maximum = _clientTopicAliasMaximum;
    _outgoingAliases.reset();
    _incomingAliases.reset();
  }

  void _onConnectionLost() {
    _stopAckWatch();
    final wasConnected = _connected;
    _connected = false;
    if (wasConnected) {
      logger.log(MqttLogLevel.warning, 'Connection lost');
    }
    _failPendingSubscribes();
    _failPendingUnsubscribes();
    // Both belong to the connection that just ended. A resumed session rebuilds
    // the queue from the session store; there is nothing to carry over.
    // Publications waiting for a send slot keep their place for the next
    // connection.
    _resumeQueue.clear();
    _flow.suspend();
  }

  /// Records that [entry]'s PUBLISH or PUBREL has just been written on the
  /// current connection, starting its [ackTimeout].
  void _markSent(AwaitingAck entry) {
    entry.markSent(_epoch, _clock.elapsedMicroseconds);
  }

  void _startAckWatch() {
    _stopAckWatch();
    if (ackTimeout <= Duration.zero) {
      return;
    }
    // Checked several times per timeout, so an overdue exchange is noticed
    // within a quarter of it, but at least once a second.
    var period = ackTimeout ~/ 4;
    if (period > const Duration(seconds: 1)) {
      period = const Duration(seconds: 1);
    } else if (period < const Duration(milliseconds: 10)) {
      period = const Duration(milliseconds: 10);
    }
    _ackWatch = Timer.periodic(period, (_) => _checkAcknowledgements());
  }

  void _stopAckWatch() {
    _ackWatch?.cancel();
    _ackWatch = null;
  }

  /// Replaces the connection when an exchange written on it has waited longer
  /// than [ackTimeout] for the broker's answer. See [ackTimeout].
  void _checkAcknowledgements() {
    if (!_connected) {
      return;
    }
    final limit = ackTimeout.inMicroseconds;
    final now = _clock.elapsedMicroseconds;
    String? overdue;
    for (final entry in _session.outgoingQos1.entries) {
      if (entry.sentEpoch == _epoch && now - entry.sentAtMicros >= limit) {
        overdue = 'PUBACK for packet ${entry.packetIdentifier}';
        break;
      }
    }
    if (overdue == null) {
      for (final entry in _session.outgoingQos2.entries) {
        if (entry.sentEpoch == _epoch && now - entry.sentAtMicros >= limit) {
          final owed = entry.state == OutgoingQos2State.publishSent
              ? 'PUBREC'
              : 'PUBCOMP';
          overdue = '$owed for packet ${entry.packetIdentifier}';
          break;
        }
      }
    }
    if (overdue == null) {
      return;
    }
    _stopAckWatch();
    final error = MqttTimeoutException(
      'No $overdue within ${ackTimeout.inMilliseconds} ms; reconnecting so '
      'the session re-sends it',
    );
    logger.log(MqttLogLevel.warning, error.message);
    _connectionManager.recycle(error, StackTrace.current);
  }

  /// Re-sends everything a resumed session still owes the broker.
  ///
  /// Section 4.4 requires re-sending every unacknowledged PUBLISH with QoS > 0
  /// and every outstanding PUBREL. Three rules shape how that is done:
  ///
  /// * MQTT-3.3.4-8 forbids delaying any packet other than a PUBLISH because
  ///   the send quota is exhausted, so PUBREL is written straight away and
  ///   takes no quota. Its PUBCOMP still replenishes the quota, clamped at the
  ///   initial value exactly as section 4.9 describes.
  /// * MQTT-4.6.0-1 and MQTT-4.6.0-4 require the original ordering, so the
  ///   passes are sorted by publish and PUBREC order rather than walked store
  ///   by store — the two stores on their own cannot reproduce the order the
  ///   application published in.
  /// * MQTT-4.9.0-2 forbids sending a PUBLISH once the quota is zero, so the
  ///   PUBLISH pass takes what quota is free and leaves the rest queued. It
  ///   must never wait: this runs before the connection is reported as
  ///   established, and a broker that resumes a session while lowering its
  ///   Receive Maximum would otherwise wedge the client in `reconnecting`
  ///   with no bound. [_pumpResume] drains the queue as acknowledgements
  ///   replenish the quota.
  void _resumeSession() {
    _resumeQueue.clear();

    final pubrels = <OutgoingQos2Entry>[
      for (final entry in _session.outgoingQos2.entries)
        if (entry.state == OutgoingQos2State.pubRelSent) entry,
    ]..sort((a, b) => a.pubrecSequence.compareTo(b.pubrecSequence));
    // The connection was established in this same turn, so these writes
    // cannot find it gone.
    for (final entry in pubrels) {
      entry.pubrelResent = true;
      _connectionManager.send(
        MqttPubrelPacket(packetIdentifier: entry.packetIdentifier),
      );
      _markSent(entry);
    }

    final publishes = <_ResumeItem>[
      for (final entry in _session.outgoingQos1.entries)
        _ResumeItem(
            entry.packetIdentifier, entry.sequence, MqttQos.atLeastOnce),
      for (final entry in _session.outgoingQos2.entries)
        if (entry.state == OutgoingQos2State.publishSent)
          _ResumeItem(
              entry.packetIdentifier, entry.sequence, MqttQos.exactlyOnce),
    ]..sort((a, b) => a.sequence.compareTo(b.sequence));
    _resumeQueue.addAll(publishes);
    _pumpResume();
  }

  /// Re-sends as many queued publications as the send quota allows.
  ///
  /// Called once from [_resumeSession] and again whenever an acknowledgement
  /// replenishes the quota, so a resume that could not finish in one pass
  /// continues without blocking anything. Queued items always have their
  /// session entry: nothing retires an entry that is waiting here without
  /// also clearing the queue.
  void _pumpResume() {
    while (_resumeQueue.isNotEmpty) {
      // MQTT-4.9.0-2: with no quota left, stop. The rest stays queued.
      if (!_flow.tryAcquire()) {
        return;
      }
      final item = _resumeQueue.removeFirst();
      try {
        _connectionManager.send(_resumePublishPacket(item));
        final resent = item.qos == MqttQos.exactlyOnce
            ? _session.outgoingQos2[item.packetIdentifier]
            : _session.outgoingQos1[item.packetIdentifier];
        if (resent != null) {
          _markSent(resent);
        }
      } on MqttPacketTooLargeException catch (error, stackTrace) {
        // The new connection's server accepts smaller packets than the one
        // this was first sent to. MQTT-3.1.2-25: discard it and carry on as
        // if it had been sent.
        logger.log(MqttLogLevel.warning,
            'Discarding packet ${item.packetIdentifier} on resume: $error');
        final completer = item.qos == MqttQos.exactlyOnce
            ? _session.outgoingQos2.remove(item.packetIdentifier)!.completer
            : _session.outgoingQos1.remove(item.packetIdentifier)!.completer;
        _session.packetIds.release(item.packetIdentifier);
        _flow.release();
        completer.completeError(error, stackTrace);
      }
    }
  }

  /// A retransmission never carries a Topic Alias: the alias mapping belongs
  /// to the connection it was made on (section 3.3.2.3.4), and the next one
  /// may allow fewer aliases or none.
  static List<MqttProperty> _withoutTopicAlias(List<MqttProperty> properties) =>
      [
        for (final p in properties)
          if (p is! TopicAlias) p
      ];

  MqttPublishPacket _resumePublishPacket(_ResumeItem item) {
    // MQTT-3.3.1-1: a re-delivery of an unacknowledged publication sets DUP.
    if (item.qos == MqttQos.exactlyOnce) {
      final entry = _session.outgoingQos2[item.packetIdentifier]!
        ..retransmitted = true;
      return MqttPublishPacket(
        topicName: entry.topic,
        payload: entry.payload,
        qos: MqttQos.exactlyOnce,
        retain: entry.retain,
        dup: true,
        packetIdentifier: entry.packetIdentifier,
        properties: _withoutTopicAlias(entry.properties),
      );
    }
    final entry = _session.outgoingQos1[item.packetIdentifier]!
      ..retransmitted = true;
    return MqttPublishPacket(
      topicName: entry.topic,
      payload: entry.payload,
      qos: MqttQos.atLeastOnce,
      retain: entry.retain,
      dup: true,
      packetIdentifier: entry.packetIdentifier,
      properties: _withoutTopicAlias(entry.properties),
    );
  }

  /// Gives a send slot back: a stalled resume uses it first (older
  /// publications), then publications waiting in [FlowController.acquire].
  void _releaseFlow() {
    _flow.release();
    _pumpResume();
    _flow.dispatch();
  }

  /// Discards the session (fresh session): fails in-flight publishes, resets
  /// state, and re-subscribes to any known subscriptions.
  void _discardSession({bool notify = true, bool clearSubscriptions = false}) {
    for (final entry in _session.outgoingQos1.entries.toList()) {
      if (!entry.completer.isCompleted) {
        entry.completer.completeError(
          MqttConnectionException('Session lost before acknowledgement'),
        );
      }
    }
    for (final entry in _session.outgoingQos2.entries.toList()) {
      if (!entry.completer.isCompleted) {
        entry.completer.completeError(
          MqttConnectionException('Session lost before acknowledgement'),
        );
      }
    }
    _session.outgoingQos1.clear();
    _session.outgoingQos2.clear();
    _session.incomingQos2.clear();
    _session.packetIds.reset();
    _resumeQueue.clear();

    final groups = clearSubscriptions
        ? const <int?, List<MqttSubscription>>{}
        : _session.subscriptions.groupedByIdentifier();
    if (clearSubscriptions) {
      _session.subscriptions.clear();
      _resubscribeIncomplete = false;
      _resubscribeRound++;
    }
    if (notify && groups.isNotEmpty) {
      _resubscribeAll(groups);
    }
  }

  /// Re-establishes subscriptions after the session was lost.
  ///
  /// A SUBSCRIBE carries at most one Subscription Identifier, so filters are
  /// re-sent grouped by the identifier they were originally registered with.
  ///
  /// [_resubscribeIncomplete] stays set until the broker has answered every
  /// packet of this round, so a later connection that finds the session
  /// present sends them again.
  void _resubscribeAll(Map<int?, List<MqttSubscription>> groups) {
    final round = ++_resubscribeRound;
    final total = groups.values.fold<int>(0, (sum, g) => sum + g.length);
    if (total == 0) {
      _resubscribeIncomplete = false;
      return;
    }
    _resubscribeIncomplete = true;
    logger.log(
      MqttLogLevel.info,
      'Re-subscribing to $total topic filter(s) in ${groups.length} packet(s)',
    );
    final sends = [
      for (final entry in groups.entries)
        _sendSubscribe(entry.value, subscriptionIdentifier: entry.key),
    ];
    // [_sendSubscribe] logs its own failures and never throws.
    unawaited(Future.wait(sends).then((answered) {
      if (round == _resubscribeRound && answered.every((a) => a)) {
        _resubscribeIncomplete = false;
      }
    }));
  }

  /// Sends one re-subscription packet. Completes with whether the broker
  /// answered it (a SUBACK, accepting or rejecting the filters); never
  /// throws.
  Future<bool> _sendSubscribe(
    List<MqttSubscription> subscriptions, {
    int? subscriptionIdentifier,
  }) async {
    final deadline = _deadline();
    final int packetIdentifier;
    try {
      packetIdentifier = await _session.packetIds.allocate(deadline: deadline);
    } on Object catch (e) {
      logger.log(
        MqttLogLevel.warning,
        'Re-subscribe could not get a packet identifier: $e',
      );
      return false;
    }
    final completer = Completer<MqttSubackPacket>();
    _pendingSubscribes[packetIdentifier] = _PendingSubscribe(
      completer: completer,
      subscriptions: subscriptions,
      subscriptionIdentifier: subscriptionIdentifier,
    );
    var keepInflight = false;
    var answered = false;
    try {
      _connectionManager.send(
        MqttSubscribePacket(
          packetIdentifier: packetIdentifier,
          subscriptions: subscriptions,
          properties: [
            if (subscriptionIdentifier != null)
              SubscriptionIdentifier(subscriptionIdentifier),
          ],
        ),
      );
      final suback = await _awaitAck(completer.future, 'SUBACK', deadline);
      answered = true;
      _throwIfSubackRejected(suback);
    } on MqttTimeoutException {
      // Same rule as [subscribeAll]: giving up on the answer is not the broker
      // confirming none is coming, and MQTT-2.2.1-4 keeps the identifier in use
      // until its SUBACK is processed. Releasing it here would let the next
      // publish reuse an identifier the broker still owes a SUBACK for.
      keepInflight = true;
      logger.log(
        MqttLogLevel.warning,
        'Re-subscribe timed out; packet identifier $packetIdentifier stays '
        'reserved until the SUBACK arrives or the connection ends',
      );
    } on Object catch (e) {
      logger.log(MqttLogLevel.warning, 'Re-subscribe failed: $e');
    } finally {
      if (!keepInflight) {
        _retirePendingSubscribe(packetIdentifier);
      }
    }
    return answered;
  }

  void _failPendingSubscribes([Object? error]) {
    final entries = _pendingSubscribes.entries.toList();
    _pendingSubscribes.clear();
    for (final entry in entries) {
      _session.packetIds.release(entry.key);
      if (!entry.value.completer.isCompleted) {
        entry.value.completer.completeError(
          error ?? MqttConnectionException('Connection lost'),
        );
      }
    }
  }

  void _failPendingUnsubscribes([Object? error]) {
    _unsubscribeCounts.clear();
    _failPending(_pendingUnsubscribes, error);
  }

  void _failPending<T>(
    Map<int, Completer<T>> pending, [
    Object? error,
  ]) {
    final entries = pending.entries.toList();
    pending.clear();
    for (final entry in entries) {
      _session.packetIds.release(entry.key);
      if (!entry.value.isCompleted) {
        entry.value.completeError(
          error ?? MqttConnectionException('Connection lost'),
        );
      }
    }
  }

  void _ensureConnected() {
    if (_closed) {
      throw MqttConnectionException('Client has been closed');
    }
    if (!_connected) {
      throw MqttConnectionException('Not connected');
    }
  }

  MqttTransport _createTransport() {
    final override = transportFactory;
    if (override != null) {
      return override();
    }
    if (useTls) {
      return TlsTransport(
        host: host,
        port: port,
        timeout: connectionTimeout,
        securityContext: securityContext,
        onBadCertificate: onBadCertificate,
        supportedProtocols: alpnProtocols,
      );
    }
    return TcpTransport(host: host, port: port, timeout: connectionTimeout);
  }

  bool _sameConnectSettings({
    required bool cleanStart,
    required bool? reconnectCleanStart,
    required Duration keepAlive,
    required Duration? sessionExpiryInterval,
    required List<MqttProperty> properties,
    required Duration connackTimeout,
    required int receiveMaximum,
    required int maximumPacketSize,
    required int topicAliasMaximum,
    required String? authenticationMethod,
    required Uint8List? authenticationData,
    required bool adoptBrokerSession,
  }) {
    return cleanStart == _cleanStart &&
        adoptBrokerSession == _adoptBrokerSession &&
        reconnectCleanStart == _reconnectCleanStart &&
        keepAlive == _keepAlive &&
        sessionExpiryInterval == _sessionExpiryInterval &&
        connackTimeout == _connackTimeout &&
        receiveMaximum == _clientReceiveMaximum &&
        maximumPacketSize == _clientMaximumPacketSize &&
        topicAliasMaximum == _clientTopicAliasMaximum &&
        authenticationMethod == _authenticationMethod &&
        _bytesEqual(authenticationData, _authenticationData) &&
        _sameProperties(properties, _connectProperties);
  }

  static bool _sameProperties(
    List<MqttProperty> left,
    List<MqttProperty> right,
  ) {
    if (left.length != right.length) {
      return false;
    }
    for (var i = 0; i < left.length; i++) {
      if (left[i] != right[i]) {
        return false;
      }
    }
    return true;
  }

  static bool _bytesEqual(Uint8List? left, Uint8List? right) {
    if (identical(left, right)) {
      return true;
    }
    if (left == null || right == null || left.length != right.length) {
      return false;
    }
    for (var i = 0; i < left.length; i++) {
      if (left[i] != right[i]) {
        return false;
      }
    }
    return true;
  }

  static int _requestedExpirySeconds(
    Duration? sessionExpiryInterval,
    List<MqttProperty> properties,
  ) {
    if (sessionExpiryInterval != null) {
      return sessionExpiryInterval.inSeconds;
    }
    for (final property in properties) {
      if (property is SessionExpiryInterval) {
        return property.seconds;
      }
    }
    return 0;
  }

  int get _keepAliveSeconds {
    if (_keepAlive <= Duration.zero) {
      return 0;
    }
    final seconds = _keepAlive.inSeconds;
    return seconds == 0 ? 1 : seconds;
  }

  bool _determineCleanStart() {
    if (!_hasConnectedOnce) {
      return _cleanStart;
    }
    if (_reconnectCleanStart != null) {
      return _reconnectCleanStart!;
    }
    // When reconnecting, attempt to resume the session if a non-zero expiry
    // interval was configured (MQTT-3.1.2-4, MQTT-3.1.2-5).
    if (_requestedSessionExpirySeconds() > 0) {
      return false;
    }
    return _cleanStart;
  }

  MqttConnectPacket _buildConnectPacket() {
    final cleanStart = _determineCleanStart();
    _lastSentCleanStart = cleanStart;
    return MqttConnectPacket(
      clientId: effectiveClientId,
      cleanStart: cleanStart,
      keepAliveSeconds: _keepAliveSeconds,
      properties: [
        if (_sessionExpiryInterval != null)
          SessionExpiryInterval(_sessionExpiryInterval!.inSeconds),
        if (_clientReceiveMaximum != 65535)
          ReceiveMaximum(_clientReceiveMaximum),
        if (_clientMaximumPacketSize != 268435455)
          MaximumPacketSize(_clientMaximumPacketSize),
        if (_clientTopicAliasMaximum != 0)
          TopicAliasMaximum(_clientTopicAliasMaximum),
        if (_authenticationMethod != null)
          AuthenticationMethod(_authenticationMethod!),
        if (_authenticationData != null)
          AuthenticationData(_authenticationData!),
        ..._connectProperties,
      ],
      will: will,
      username: username,
      password: password,
    );
  }

  static String _generateClientId() {
    final random = Random.secure();
    final suffix = List.generate(8, (_) => random.nextInt(10)).join();
    return 'mqtt5-$suffix';
  }
}

/// Logging is an observer boundary. A broken application logger must not
/// interrupt packet handling, reconnect cleanup or the keep-alive timer.
final class _GuardedMqttLogger implements MqttLogger {
  const _GuardedMqttLogger(this.delegate);

  final MqttLogger delegate;

  @override
  void log(MqttLogLevel level, String message) {
    try {
      delegate.log(level, message);
    } on Object {
      // There is deliberately no fallback logger here: it could fail for the
      // same reason (for example a closed stdout pipe).
    }
  }
}

/// One publication a resumed session still owes the broker.
///
/// Holds the identifier rather than the entry so the pump always re-reads the
/// session store: an entry can be retired while it waits for send quota.
final class _ResumeItem {
  const _ResumeItem(this.packetIdentifier, this.sequence, this.qos);

  final int packetIdentifier;
  final int sequence;
  final MqttQos qos;
}

/// The topic name and properties to put on the wire for one publish, plus the
/// alias binding to record once that publish has actually been written.
final class _Aliased {
  _Aliased(this.topic, this.properties, this._pending, this._aliases);

  final String topic;
  final List<MqttProperty> properties;
  final ({int alias, String topic})? _pending;
  final TopicAliasMap _aliases;

  /// Binds a newly reserved alias, now that the broker has seen the full
  /// topic name. A no-op when no new alias was reserved.
  void commit() {
    final pending = _pending;
    if (pending != null) {
      _aliases.commit(pending.alias, pending.topic);
    }
  }
}

final class _PendingSubscribe {
  _PendingSubscribe({
    required this.completer,
    required this.subscriptions,
    this.subscriptionIdentifier,
  });

  final Completer<MqttSubackPacket> completer;
  final List<MqttSubscription> subscriptions;
  final int? subscriptionIdentifier;
}
