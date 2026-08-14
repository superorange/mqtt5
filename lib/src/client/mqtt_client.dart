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
  }) : clientId = clientId ?? _generateClientId() {
    _connectionManager = ConnectionManager(
      transportFactory: _createTransport,
      onPacket: _onPacket,
      onConnected: _onConnected,
      onConnectionLost: _onConnectionLost,
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

  /// Overrides transport creation; intended for tests and custom transports.
  final MqttTransport Function()? transportFactory;

  late final ConnectionManager _connectionManager;
  final StreamController<MqttMessage> _messages =
      StreamController<MqttMessage>.broadcast(sync: true);

  final MqttSession _session = MqttSession();
  final Map<int, Completer<MqttSubackPacket>> _pendingSubscribes = {};
  final Map<int, Completer<MqttUnsubackPacket>> _pendingUnsubscribes = {};

  bool _connected = false;
  bool _sessionPresent = false;

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
      _discardSession(notify: false, clearSubscriptions: true);
    }

    await _connectionManager.start(_buildConnectPacket());
  }

  /// Sends a DISCONNECT packet and closes the connection.
  Future<void> disconnect({
    MqttReasonCode reasonCode = MqttReasonCode.success,
    List<MqttProperty> properties = const [],
  }) async {
    _connected = false;
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
  }

  /// Subscribes to [topicFilter], completing when the broker acknowledges.
  ///
  /// Throws [MqttServerRejectedException] if the broker rejects any filter.
  Future<void> subscribe(
    String topicFilter, {
    MqttSubscriptionOptions options = const MqttSubscriptionOptions(),
    int? subscriptionIdentifier,
  }) async {
    _validateSubscriptionTopic(topicFilter);
    final packetIdentifier = await _session.packetIds.allocate();
    final completer = Completer<MqttSubackPacket>();
    _pendingSubscribes[packetIdentifier] = completer;
    try {
      _connectionManager.send(
        MqttSubscribePacket(
          packetIdentifier: packetIdentifier,
          subscriptions: [MqttSubscription(topicFilter, options: options)],
          properties: [
            if (subscriptionIdentifier != null)
              SubscriptionIdentifier(subscriptionIdentifier),
          ],
        ),
      );
      final suback = await completer.future;
      _throwIfSubackRejected(suback);
      _session.subscriptions.add(MqttSubscription(topicFilter, options: options));
    } finally {
      _pendingSubscribes.remove(packetIdentifier);
      _session.packetIds.release(packetIdentifier);
    }
  }

  /// Subscribes to multiple topic filters in a single SUBSCRIBE packet.
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
      final suback = await completer.future;
      _throwIfSubackRejected(suback);
      for (final subscription in subscriptions) {
        _session.subscriptions.add(subscription);
      }
    } finally {
      _pendingSubscribes.remove(packetIdentifier);
      _session.packetIds.release(packetIdentifier);
    }
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
      final unsuback = await completer.future;
      for (final reasonCode in unsuback.reasonCodes) {
        if (reasonCode >= 0x80) {
          throw MqttServerRejectedException(
            reasonCode,
            'Unsubscribe failed',
          );
        }
      }
      for (final topicFilter in topicFilters) {
        _session.subscriptions.remove(topicFilter);
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
    metrics.messagesPublished++;
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
        return const MqttPublishResult();
      case MqttQos.atLeastOnce:
        return _publishQos1(topic, payload, retain: retain, properties: properties);
      case MqttQos.exactlyOnce:
        return _publishQos2(topic, payload, retain: retain, properties: properties);
    }
  }

  /// Applies the client-to-server Topic Alias mapping to an outgoing publish.
  ///
  /// Returns the possibly-aliased topic name and the properties to send. The
  /// entry's stored topic/properties are always the original values so a
  /// retransmit after reconnect uses the full topic name.
  ({String topic, List<MqttProperty> properties}) _applyOutgoingAlias(
    String topic,
    List<MqttProperty> properties,
  ) {
    if (properties.any((p) => p is TopicAlias)) {
      return (topic: topic, properties: properties);
    }
    final existing = _outgoingAliases.aliasFor(topic);
    if (existing != null) {
      return (topic: '', properties: [...properties, TopicAlias(existing)]);
    }
    final assigned = _outgoingAliases.assign(topic);
    if (assigned != null) {
      return (topic: topic, properties: [...properties, TopicAlias(assigned)]);
    }
    return (topic: topic, properties: properties);
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
      return await entry.completer.future;
    } catch (_) {
      _flow.release();
      rethrow;
    } finally {
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
      return await entry.completer.future;
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
        final reasonCode = disconnect.reasonCode;
        if (reasonCode != null &&
            (reasonCode == MqttReasonCode.useAnotherServer ||
                reasonCode == MqttReasonCode.serverMoved)) {
          String? serverReference;
          for (final property in disconnect.properties) {
            if (property is ServerReference) {
              serverReference = property.value;
            }
          }
          onServerMoved?.call(serverReference, reasonCode);
        }
        logger.log(
          MqttLogLevel.warning,
          'Broker sent DISCONNECT: $reasonCode',
        );
      default:
        break;
    }
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
    _session.packetIds.release(puback.packetIdentifier);
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
      _session.packetIds.release(pubrec.packetIdentifier);
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
    _session.packetIds.release(pubcomp.packetIdentifier);
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
      unawaited(_resumeSession());
    } else {
      _discardSession();
    }
  }

  void _applyServerCapabilities(MqttConnackPacket connack) {
    for (final property in connack.properties) {
      switch (property) {
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
      final suback = await completer.future;
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
      clientId: clientId,
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
