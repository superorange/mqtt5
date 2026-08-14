import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

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
import '../session/mqtt_session.dart';
import '../session/outgoing_qos1.dart';
import '../session/outgoing_qos2.dart';
import '../session/topic_alias.dart';
import '../subscription.dart';
import '../transport/mqtt_transport.dart';
import '../transport/tcp_transport.dart';
import '../transport/tls_transport.dart';
import 'connection_manager.dart';
import 'flow_controller.dart';
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
    this.logger = const SilentLogger(),
    this.reconnectManager,
    this.authenticator,
    this.transportFactory,
    this.autoReconnect = true,
    this.operationTimeout = const Duration(seconds: 30),
  }) : clientId = clientId ?? _generateClientId() {
    _connectionManager = ConnectionManager(
      transportFactory: _createTransport,
      onPacket: _onPacket,
      onConnected: _onConnected,
      onConnectionLost: _onConnectionLost,
      onFatalError: _onFatalError,
      autoReconnect: autoReconnect,
      logger: logger,
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

  /// How long [publish], [subscribe] and [unsubscribe] wait for the broker's
  /// acknowledgement before failing with [MqttTimeoutException].
  ///
  /// [Duration.zero] disables the timeout and waits indefinitely.
  final Duration operationTimeout;

  /// Overrides transport creation; intended for tests and custom transports.
  final MqttTransport Function()? transportFactory;

  late final ConnectionManager _connectionManager;
  // Deliberately not a sync controller: a subscriber that throws must not
  // unwind into the socket event handler and be mistaken for a peer error.
  final StreamController<MqttMessage> _messages =
      StreamController<MqttMessage>.broadcast();
  final StreamController<Object> _errors =
      StreamController<Object>.broadcast();

  final MqttSession _session = MqttSession();
  final Map<int, Completer<MqttSubackPacket>> _pendingSubscribes = {};
  final Map<int, Completer<MqttUnsubackPacket>> _pendingUnsubscribes = {};

  bool _connected = false;
  bool _sessionPresent = false;
  bool _closed = false;
  String? _assignedClientId;

  final ServerCapabilities _capabilities = ServerCapabilities();
  final FlowController _flow = FlowController();
  final TopicAliasMap _outgoingAliases = TopicAliasMap();
  final TopicAliasMap _incomingAliases = TopicAliasMap();
  int _clientReceiveMaximum = 65535;
  int _clientMaximumPacketSize = 268435455;
  int _clientTopicAliasMaximum = 0;

  // Connect settings, retained for reconnect and rebuilds.
  bool _cleanStart = true;
  Duration _keepAlive = const Duration(seconds: 60);
  Duration? _sessionExpiryInterval;
  List<MqttProperty> _connectProperties = const [];
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
  Stream<MqttMessage> get messages => _messages.stream;

  /// Errors that ended the connection for good.
  ///
  /// A failure on the initial [connect] is thrown from that call. Once the
  /// client is running, there is no caller left to throw to, so a rejection
  /// or an unrecoverable failure on a later reconnect is reported here.
  Stream<Object> get errors => _errors.stream;

  /// The client identifier in use, which is the one the broker assigned in
  /// CONNACK when the client connected with an empty [clientId].
  String get effectiveClientId => _assignedClientId ?? clientId;

  /// Establishes (and maintains) the MQTT connection.
  Future<void> connect({
    bool cleanStart = true,
    Duration keepAlive = const Duration(seconds: 60),
    Duration? sessionExpiryInterval,
    List<MqttProperty> properties = const [],
    Duration connackTimeout = const Duration(seconds: 10),
    int receiveMaximum = 65535,
    int maximumPacketSize = 268435455,
    int topicAliasMaximum = 0,
    String? authenticationMethod,
    Uint8List? authenticationData,
  }) async {
    if (_closed) {
      throw MqttConnectionException('Client has been closed');
    }
    if (keepAlive.inSeconds > 0xFFFF) {
      throw ArgumentError.value(
        keepAlive,
        'keepAlive',
        'Keep Alive must not exceed 65535 seconds (18h12m15s)',
      );
    }
    _cleanStart = cleanStart;
    _keepAlive = keepAlive;
    _sessionExpiryInterval = sessionExpiryInterval;
    _connectProperties = properties;
    _clientReceiveMaximum = receiveMaximum;
    _clientMaximumPacketSize = maximumPacketSize;
    _clientTopicAliasMaximum = topicAliasMaximum;
    _authenticationMethod = authenticationMethod;
    _authenticationData = authenticationData;
    _connectionManager.connackTimeout = connackTimeout;
    _connectionManager.clientMaximumPacketSize = maximumPacketSize;
    _connectionManager.authenticator = authenticator;

    if (cleanStart) {
      _assignedClientId = null;
      _discardSession(notify: false, clearSubscriptions: true);
    }

    // Passing a builder rather than a packet so every reconnect picks up the
    // broker-assigned client identifier and the current session settings.
    await _connectionManager.start(_buildConnectPacket);
  }

  /// Sends a DISCONNECT packet and closes the connection.
  ///
  /// Operations still awaiting an acknowledgement are failed with
  /// [MqttConnectionException]; nothing is left pending.
  Future<void> disconnect({
    MqttReasonCode reasonCode = MqttReasonCode.success,
    List<MqttProperty> properties = const [],
  }) async {
    _connected = false;
    _connectionManager.beginDisconnect();
    try {
      _connectionManager.send(
        MqttDisconnectPacket(reasonCode: reasonCode, properties: properties),
      );
    } on MqttException catch (e) {
      logger.log(MqttLogLevel.debug, 'DISCONNECT send failed: $e');
    }
    _flow.reset();
    // stop() closes the transport, which flushes the queued DISCONNECT.
    await _connectionManager.stop();
    _abortPending(
      MqttConnectionException('Disconnected before acknowledgement'),
    );
  }

  /// Disconnects and releases every resource held by the client.
  ///
  /// The client cannot be reconnected afterwards; [messages], [stateStream]
  /// and [errors] are closed.
  Future<void> close({
    MqttReasonCode reasonCode = MqttReasonCode.success,
  }) async {
    if (_closed) {
      return;
    }
    _closed = true;
    await disconnect(reasonCode: reasonCode);
    await _connectionManager.dispose();
    await _messages.close();
    await _errors.close();
  }

  /// Fails every operation waiting for a broker acknowledgement.
  void _abortPending(Object error) {
    _failPending(_pendingSubscribes);
    _failPending(_pendingUnsubscribes);
    for (final entry in _session.outgoingQos1.entries.toList()) {
      if (!entry.completer.isCompleted) {
        entry.completer.completeError(error);
      }
    }
    for (final entry in _session.outgoingQos2.entries.toList()) {
      if (!entry.completer.isCompleted) {
        entry.completer.completeError(error);
      }
    }
  }

  void _onFatalError(Object error, StackTrace stackTrace) {
    _connected = false;
    _abortPending(error);
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
    if (subscriptions.isEmpty) {
      return;
    }
    for (final subscription in subscriptions) {
      _validateSubscriptionTopic(subscription.topicFilter);
    }
    final packetIdentifier = await _session.packetIds.allocate();
    final completer = Completer<MqttSubackPacket>();
    _pendingSubscribes[packetIdentifier] = completer;
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
      final suback = await _awaitAck(completer.future, 'SUBACK');
      if (suback.reasonCodes.length != subscriptions.length) {
        throw MqttProtocolException(
          'SUBACK carries ${suback.reasonCodes.length} reason code(s) for '
          '${subscriptions.length} topic filter(s)',
        );
      }
      for (var i = 0; i < subscriptions.length; i++) {
        if (suback.reasonCodes[i] < 0x80) {
          _session.subscriptions.add(subscriptions[i]);
        }
      }
      _throwIfSubackRejected(suback);
    } finally {
      _pendingSubscribes.remove(packetIdentifier);
      _session.packetIds.release(packetIdentifier);
    }
  }

  /// Waits for a broker acknowledgement, applying [operationTimeout].
  Future<T> _awaitAck<T>(Future<T> future, String what) {
    if (operationTimeout <= Duration.zero) {
      return future;
    }
    return future.timeout(
      operationTimeout,
      onTimeout: () => throw MqttTimeoutException(
        'Timed out after ${operationTimeout.inSeconds}s waiting for $what',
      ),
    );
  }

  void _validateSubscriptionTopic(String topicFilter) {
    if (topicFilter.startsWith(r'$share/') &&
        !_capabilities.sharedSubscriptionAvailable) {
      throw MqttFlowControlException(
        'Shared subscriptions are not supported by the server',
      );
    }
    if ((topicFilter.contains('+') || topicFilter.contains('#')) &&
        !_capabilities.wildcardSubscriptionAvailable) {
      throw MqttFlowControlException(
        'Wildcard subscriptions are not supported by the server',
      );
    }
  }

  /// Unsubscribes from [topicFilters], completing when the broker acknowledges.
  Future<void> unsubscribe(List<String> topicFilters) async {
    if (topicFilters.isEmpty) {
      return;
    }
    final packetIdentifier = await _session.packetIds.allocate();
    final completer = Completer<MqttUnsubackPacket>();
    _pendingUnsubscribes[packetIdentifier] = completer;
    try {
      _connectionManager.send(
        MqttUnsubscribePacket(
          packetIdentifier: packetIdentifier,
          topicFilters: topicFilters,
        ),
      );
      final unsuback = await _awaitAck(completer.future, 'UNSUBACK');
      if (unsuback.reasonCodes.length != topicFilters.length) {
        throw MqttProtocolException(
          'UNSUBACK carries ${unsuback.reasonCodes.length} reason code(s) for '
          '${topicFilters.length} topic filter(s)',
        );
      }
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
    } finally {
      _pendingUnsubscribes.remove(packetIdentifier);
      _session.packetIds.release(packetIdentifier);
    }
  }

  /// Publishes [payload] to [topic].
  ///
  /// For QoS 0 the future completes once the packet is written. For QoS 1 and
  /// QoS 2 it completes once the broker's acknowledgement completes the
  /// protocol exchange.
  Future<MqttPublishResult> publish(
    String topic,
    Uint8List payload, {
    MqttQos qos = MqttQos.atMostOnce,
    bool retain = false,
    List<MqttProperty> properties = const [],
  }) async {
    _validatePublishTopic(topic);
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
        return _publishQos1(topic, payload, retain: retain, properties: properties);
      case MqttQos.exactlyOnce:
        return _publishQos2(topic, payload, retain: retain, properties: properties);
    }
  }

  /// Rejects topic names that the broker is required to reject, so the caller
  /// gets a local error instead of being disconnected.
  void _validatePublishTopic(String topic) {
    if (topic.contains('+') || topic.contains('#')) {
      throw ArgumentError.value(
        topic,
        'topic',
        'A publish topic name must not contain wildcards',
      );
    }
    if (topic.isEmpty) {
      throw ArgumentError.value(topic, 'topic', 'Topic name must not be empty');
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
    if (properties.any((p) => p is TopicAlias)) {
      return _Aliased(topic, properties, null, _outgoingAliases);
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

  Future<MqttPublishResult> _publishQos1(
    String topic,
    Uint8List payload, {
    required bool retain,
    required List<MqttProperty> properties,
  }) async {
    final packetIdentifier = await _session.packetIds.allocate();
    await _flow.acquire();
    final entry = OutgoingQos1Entry(
      packetIdentifier: packetIdentifier,
      topic: topic,
      payload: payload,
      retain: retain,
      properties: properties,
    );
    _session.outgoingQos1.put(entry);
    try {
      final aliased = _applyOutgoingAlias(topic, properties);
      _connectionManager.send(
        MqttPublishPacket(
          topicName: aliased.topic,
          payload: payload,
          qos: MqttQos.atLeastOnce,
          retain: retain,
          packetIdentifier: packetIdentifier,
          properties: aliased.properties,
        ),
      );
      aliased.commit();
      return await _awaitAck(entry.completer.future, 'PUBACK');
    } catch (_) {
      _flow.release();
      rethrow;
    } finally {
      // Single owner of both resources: the acknowledgement handlers never
      // release them, so an identifier cannot be handed out again while this
      // call still refers to it.
      _session.outgoingQos1.remove(packetIdentifier);
      _session.packetIds.release(packetIdentifier);
    }
  }

  Future<MqttPublishResult> _publishQos2(
    String topic,
    Uint8List payload, {
    required bool retain,
    required List<MqttProperty> properties,
  }) async {
    final packetIdentifier = await _session.packetIds.allocate();
    await _flow.acquire();
    final entry = OutgoingQos2Entry(
      packetIdentifier: packetIdentifier,
      topic: topic,
      payload: payload,
      retain: retain,
      properties: properties,
    );
    _session.outgoingQos2.put(entry);
    try {
      final aliased = _applyOutgoingAlias(topic, properties);
      _connectionManager.send(
        MqttPublishPacket(
          topicName: aliased.topic,
          payload: payload,
          qos: MqttQos.exactlyOnce,
          retain: retain,
          packetIdentifier: packetIdentifier,
          properties: aliased.properties,
        ),
      );
      aliased.commit();
      return await _awaitAck(entry.completer.future, 'PUBCOMP');
    } catch (_) {
      _flow.release();
      rethrow;
    } finally {
      _session.outgoingQos2.remove(packetIdentifier);
      _session.packetIds.release(packetIdentifier);
    }
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
        _completePending(_pendingSubscribes, suback.packetIdentifier, suback);
      case MqttUnsubackPacket unsuback:
        _completePending(_pendingUnsubscribes, unsuback.packetIdentifier, unsuback);
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
    _connectionManager.serverReference = serverReference;
    logger.log(MqttLogLevel.warning, 'Broker sent DISCONNECT: $reasonCode');
    if (reasonCode == MqttReasonCode.useAnotherServer ||
        reasonCode == MqttReasonCode.serverMoved) {
      onServerMoved?.call(serverReference, reasonCode!);
    }
    _connected = false;
    unawaited(_connectionManager.handleServerDisconnect(reasonCode));
  }

  void _completePending<T>(
    Map<int, Completer<T>> pending,
    int packetIdentifier,
    T value,
  ) {
    final completer = pending.remove(packetIdentifier);
    if (completer != null && !completer.isCompleted) {
      completer.complete(value);
    }
  }

  void _handlePublish(MqttPublishPacket publish) {
    final topic = _resolveIncomingTopic(publish);
    switch (publish.qos) {
      case MqttQos.atMostOnce:
        _deliver(publish, topic);
      case MqttQos.atLeastOnce:
        _deliver(publish, topic);
        _connectionManager.send(
          MqttPubackPacket(packetIdentifier: publish.packetIdentifier),
        );
      case MqttQos.exactlyOnce:
        _handleIncomingQos2(publish, topic);
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
      return publish.topicName;
    }
    final alias = aliasProperty.value;
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

  void _handleIncomingQos2(MqttPublishPacket publish, String topic) {
    final isNew = _session.incomingQos2.add(publish.packetIdentifier);
    if (isNew) {
      _deliver(publish, topic);
    }
    _connectionManager.send(
      MqttPubrecPacket(packetIdentifier: publish.packetIdentifier),
    );
  }

  void _handlePuback(MqttPubackPacket puback) {
    final entry = _session.outgoingQos1.remove(puback.packetIdentifier);
    if (entry == null) {
      return;
    }
    _flow.release();
    entry.completer.complete(
      MqttPublishResult(
        reasonCode: puback.reasonCode ?? MqttReasonCode.success,
        properties: puback.properties,
      ),
    );
  }

  void _handlePubrec(MqttPubrecPacket pubrec) {
    final entry = _session.outgoingQos2[pubrec.packetIdentifier];
    if (entry == null) {
      return;
    }
    final reasonCode = pubrec.reasonCode;
    if (reasonCode != null && reasonCode.value >= 0x80) {
      _session.outgoingQos2.remove(pubrec.packetIdentifier);
      _flow.release();
      entry.completer.complete(
        MqttPublishResult(reasonCode: reasonCode, properties: pubrec.properties),
      );
      return;
    }
    entry.state = OutgoingQos2State.pubRecReceived;
    _connectionManager.send(
      MqttPubrelPacket(packetIdentifier: pubrec.packetIdentifier),
    );
    entry.state = OutgoingQos2State.pubRelSent;
  }

  void _handlePubrel(MqttPubrelPacket pubrel) {
    final known = _session.incomingQos2.contains(pubrel.packetIdentifier);
    _session.incomingQos2.remove(pubrel.packetIdentifier);
    _connectionManager.send(
      MqttPubcompPacket(
        packetIdentifier: pubrel.packetIdentifier,
        reasonCode:
            known ? null : MqttReasonCode.packetIdentifierNotFound,
      ),
    );
  }

  void _handlePubcomp(MqttPubcompPacket pubcomp) {
    final entry = _session.outgoingQos2.remove(pubcomp.packetIdentifier);
    if (entry == null) {
      return;
    }
    _flow.release();
    entry.completer.complete(
      MqttPublishResult(
        reasonCode: pubcomp.reasonCode ?? MqttReasonCode.success,
        properties: pubcomp.properties,
      ),
    );
  }

  void _deliver(MqttPublishPacket publish, String topic) {
    metrics.messagesReceived++;
    _messages.add(
      MqttMessage(
        topic: topic,
        payload: publish.payload,
        qos: publish.qos,
        retain: publish.retain,
        duplicate: publish.dup,
        properties: publish.properties,
      ),
    );
  }

  void _onConnected(MqttConnackPacket connack) {
    _connected = true;
    _sessionPresent = connack.sessionPresent;
    _applyServerCapabilities(connack);
    logger.log(
      MqttLogLevel.info,
      'Connected (sessionPresent=${connack.sessionPresent})',
    );
    if (connack.sessionPresent) {
      // Fire and forget: a failure here means the connection dropped again,
      // which the transport reports on its own.
      unawaited(_resumeSession().catchError((Object e) {
        logger.log(MqttLogLevel.warning, 'Session resume interrupted: $e');
      }));
    } else {
      _discardSession();
    }
  }

  void _applyServerCapabilities(MqttConnackPacket connack) {
    // Capabilities are per-connection: an absent property means "default",
    // so start from the defaults instead of keeping the previous connection's.
    _capabilities.reset();
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
        case RequestResponseInformation requestResponse:
          _capabilities.requestResponseInformation = requestResponse.value == 1;
        default:
          break;
      }
    }
    _flow.receiveMaximum = _capabilities.receiveMaximum;
    _connectionManager.maximumPacketSize = _capabilities.maximumPacketSize;
    _outgoingAliases.maximum = _capabilities.topicAliasMaximum;
    _incomingAliases.maximum = _clientTopicAliasMaximum;
    _outgoingAliases.reset();
    _incomingAliases.reset();
  }

  void _onConnectionLost() {
    final wasConnected = _connected;
    _connected = false;
    if (wasConnected) {
      logger.log(MqttLogLevel.warning, 'Connection lost');
    }
    _failPending(_pendingSubscribes);
    _failPending(_pendingUnsubscribes);
    _flow.reset();
  }

  /// Resumes the session after the broker reported it was present: retransmit
  /// unacknowledged PUBLISH packets (DUP=1) and outstanding PUBREL packets.
  Future<void> _resumeSession() async {
    for (final entry in _session.outgoingQos1.entries) {
      await _flow.acquire();
      entry.duplicate = true;
      _connectionManager.send(
        MqttPublishPacket(
          topicName: entry.topic,
          payload: entry.payload,
          qos: MqttQos.atLeastOnce,
          retain: entry.retain,
          dup: true,
          packetIdentifier: entry.packetIdentifier,
          properties: entry.properties,
        ),
      );
    }
    for (final entry in _session.outgoingQos2.entries) {
      await _flow.acquire();
      if (entry.state == OutgoingQos2State.publishSent) {
        entry.duplicate = true;
        _connectionManager.send(
          MqttPublishPacket(
            topicName: entry.topic,
            payload: entry.payload,
            qos: MqttQos.exactlyOnce,
            retain: entry.retain,
            dup: true,
            packetIdentifier: entry.packetIdentifier,
            properties: entry.properties,
          ),
        );
      } else {
        _connectionManager.send(
          MqttPubrelPacket(packetIdentifier: entry.packetIdentifier),
        );
      }
    }
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
    _flow.reset();

    final subscriptions = clearSubscriptions
        ? const <MqttSubscription>[]
        : _session.subscriptions.all;
    if (clearSubscriptions) {
      _session.subscriptions.clear();
    }
    if (notify && subscriptions.isNotEmpty && _connected) {
      _resubscribeAll(subscriptions);
    }
  }

  /// Re-establishes subscriptions after the session was lost.
  void _resubscribeAll(List<MqttSubscription> subscriptions) {
    if (subscriptions.isEmpty) {
      return;
    }
    logger.log(
      MqttLogLevel.info,
      'Re-subscribing to ${subscriptions.length} topic filter(s)',
    );
    // Fire and forget; failures surface in the log.
    unawaited(_sendSubscribe(subscriptions));
  }

  Future<void> _sendSubscribe(List<MqttSubscription> subscriptions) async {
    final packetIdentifier = await _session.packetIds.allocate();
    final completer = Completer<MqttSubackPacket>();
    _pendingSubscribes[packetIdentifier] = completer;
    try {
      _connectionManager.send(
        MqttSubscribePacket(
          packetIdentifier: packetIdentifier,
          subscriptions: subscriptions,
        ),
      );
      final suback = await _awaitAck(completer.future, 'SUBACK');
      _throwIfSubackRejected(suback);
    } on MqttException catch (e) {
      logger.log(MqttLogLevel.warning, 'Re-subscribe failed: $e');
    } finally {
      _pendingSubscribes.remove(packetIdentifier);
      _session.packetIds.release(packetIdentifier);
    }
  }

  void _failPending<T>(Map<int, Completer<T>> pending) {
    final completers = pending.values.toList();
    pending.clear();
    for (final completer in completers) {
      if (!completer.isCompleted) {
        completer.completeError(MqttConnectionException('Connection lost'));
      }
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

  int get _keepAliveSeconds {
    if (_keepAlive <= Duration.zero) {
      return 0;
    }
    final seconds = _keepAlive.inSeconds;
    return seconds == 0 ? 1 : seconds;
  }

  MqttConnectPacket _buildConnectPacket() {
    return MqttConnectPacket(
      clientId: effectiveClientId,
      cleanStart: _cleanStart,
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
