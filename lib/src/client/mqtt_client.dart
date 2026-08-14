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
import '../subscription.dart';
import '../transport/mqtt_transport.dart';
import '../transport/tcp_transport.dart';
import '../transport/tls_transport.dart';
import 'connection_manager.dart';
import 'mqtt_connection_state.dart';
import 'mqtt_message.dart';
import 'mqtt_publish_result.dart';
import 'reconnect_manager.dart';

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

  // Connect settings, retained for reconnect and rebuilds.
  bool _cleanStart = true;
  Duration _keepAlive = const Duration(seconds: 60);
  Duration? _sessionExpiryInterval;
  List<MqttProperty> _connectProperties = const [];

  MqttConnectionState get state => _connectionManager.state;

  Stream<MqttConnectionState> get stateStream => _connectionManager.stateStream;

  /// Whether the broker resumed a previous session on the last CONNACK.
  bool get sessionPresent => _sessionPresent;

  /// Incoming application messages.
  Stream<MqttMessage> get messages => _messages.stream;

  /// Establishes (and maintains) the MQTT connection.
  Future<void> connect({
    bool cleanStart = true,
    Duration keepAlive = const Duration(seconds: 60),
    Duration? sessionExpiryInterval,
    List<MqttProperty> properties = const [],
    Duration connackTimeout = const Duration(seconds: 10),
  }) async {
    _cleanStart = cleanStart;
    _keepAlive = keepAlive;
    _sessionExpiryInterval = sessionExpiryInterval;
    _connectProperties = properties;
    _connectionManager.connackTimeout = connackTimeout;

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
      await _connectionManager.flush();
    } on MqttException catch (e) {
      logger.log(MqttLogLevel.debug, 'DISCONNECT send failed: $e');
    }
    await _connectionManager.stop();
  }

  /// Subscribes to [topicFilter], completing when the broker acknowledges.
  ///
  /// Throws [MqttServerRejectedException] if the broker rejects any filter.
  Future<void> subscribe(
    String topicFilter, {
    MqttSubscriptionOptions options = const MqttSubscriptionOptions(),
  }) async {
    final packetIdentifier = await _session.packetIds.allocate();
    final completer = Completer<MqttSubackPacket>();
    _pendingSubscribes[packetIdentifier] = completer;
    try {
      _connectionManager.send(
        MqttSubscribePacket(
          packetIdentifier: packetIdentifier,
          subscriptions: [MqttSubscription(topicFilter, options: options)],
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
  Future<void> subscribeAll(List<MqttSubscription> subscriptions) async {
    if (subscriptions.isEmpty) {
      return;
    }
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
      for (final subscription in subscriptions) {
        _session.subscriptions.add(subscription);
      }
    } finally {
      _pendingSubscribes.remove(packetIdentifier);
      _session.packetIds.release(packetIdentifier);
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
    switch (qos) {
      case MqttQos.atMostOnce:
        _connectionManager.send(
          MqttPublishPacket(
            topicName: topic,
            payload: payload,
            qos: qos,
            retain: retain,
            properties: properties,
          ),
        );
        return const MqttPublishResult();
      case MqttQos.atLeastOnce:
        return _publishQos1(topic, payload, retain: retain, properties: properties);
      case MqttQos.exactlyOnce:
        return _publishQos2(topic, payload, retain: retain, properties: properties);
    }
  }

  Future<MqttPublishResult> _publishQos1(
    String topic,
    Uint8List payload, {
    required bool retain,
    required List<MqttProperty> properties,
  }) async {
    final packetIdentifier = await _session.packetIds.allocate();
    final entry = OutgoingQos1Entry(
      packetIdentifier: packetIdentifier,
      topic: topic,
      payload: payload,
      retain: retain,
      properties: properties,
    );
    _session.outgoingQos1.put(entry);
    try {
      _connectionManager.send(
        MqttPublishPacket(
          topicName: topic,
          payload: payload,
          qos: MqttQos.atLeastOnce,
          retain: retain,
          packetIdentifier: packetIdentifier,
          properties: properties,
        ),
      );
      return await entry.completer.future;
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
    final entry = OutgoingQos2Entry(
      packetIdentifier: packetIdentifier,
      topic: topic,
      payload: payload,
      retain: retain,
      properties: properties,
    );
    _session.outgoingQos2.put(entry);
    try {
      _connectionManager.send(
        MqttPublishPacket(
          topicName: topic,
          payload: payload,
          qos: MqttQos.exactlyOnce,
          retain: retain,
          packetIdentifier: packetIdentifier,
          properties: properties,
        ),
      );
      return await entry.completer.future;
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
        logger.log(
          MqttLogLevel.warning,
          'Broker sent DISCONNECT: ${disconnect.reasonCode}',
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
    switch (publish.qos) {
      case MqttQos.atMostOnce:
        _deliver(publish);
      case MqttQos.atLeastOnce:
        _deliver(publish);
        _connectionManager.send(
          MqttPubackPacket(packetIdentifier: publish.packetIdentifier),
        );
      case MqttQos.exactlyOnce:
        _handleIncomingQos2(publish);
    }
  }

  void _handleIncomingQos2(MqttPublishPacket publish) {
    final isNew = _session.incomingQos2.add(publish.packetIdentifier);
    if (isNew) {
      _deliver(publish);
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
    entry.completer.complete(
      MqttPublishResult(
        reasonCode: pubcomp.reasonCode ?? MqttReasonCode.success,
        properties: pubcomp.properties,
      ),
    );
  }

  void _deliver(MqttPublishPacket publish) {
    _messages.add(
      MqttMessage(
        topic: publish.topicName,
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
    logger.log(
      MqttLogLevel.info,
      'Connected (sessionPresent=${connack.sessionPresent})',
    );
    if (connack.sessionPresent) {
      _resumeSession();
    } else {
      _discardSession();
    }
  }

  void _onConnectionLost() {
    final wasConnected = _connected;
    _connected = false;
    if (wasConnected) {
      logger.log(MqttLogLevel.warning, 'Connection lost');
    }
    _failPending(_pendingSubscribes);
    _failPending(_pendingUnsubscribes);
  }

  /// Resumes the session after the broker reported it was present: retransmit
  /// unacknowledged PUBLISH packets (DUP=1) and outstanding PUBREL packets.
  void _resumeSession() {
    for (final entry in _session.outgoingQos1.entries) {
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
      switch (entry.state) {
        case OutgoingQos2State.publishSent:
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
        case OutgoingQos2State.pubRecReceived:
        case OutgoingQos2State.pubRelSent:
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

  MqttConnectPacket _buildConnectPacket() {
    return MqttConnectPacket(
      clientId: clientId,
      cleanStart: _cleanStart,
      keepAliveSeconds: _keepAlive.inSeconds,
      properties: [
        if (_sessionExpiryInterval != null)
          SessionExpiryInterval(_sessionExpiryInterval!.inSeconds),
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
