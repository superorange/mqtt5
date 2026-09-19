import 'dart:async';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:mqtt5/testing.dart';
import 'package:test/test.dart';

/// Regressions for the defects found by cross-checking two independent audits
/// of this package against the MQTT 5.0 specification.
Uint8List _connack({
  bool sessionPresent = false,
  MqttReasonCode reasonCode = MqttReasonCode.success,
  List<MqttProperty> properties = const [],
}) =>
    MqttPacketCodec.encode(
      MqttConnackPacket(
        sessionPresent: sessionPresent,
        reasonCode: reasonCode,
        properties: properties,
      ),
    );

Uint8List _publish(String topic, String payload) => MqttPacketCodec.encode(
      MqttPublishPacket(
        topicName: topic,
        payload: Uint8List.fromList(payload.codeUnits),
      ),
    );

Uint8List _concat(List<Uint8List> parts) {
  final builder = BytesBuilder();
  for (final part in parts) {
    builder.add(part);
  }
  return builder.takeBytes();
}

Future<void> _settle([int turns = 12]) async {
  for (var i = 0; i < turns; i++) {
    await Future<void>.delayed(Duration.zero);
  }
}

/// Decodes every packet in [bytes], which may hold more than one.
List<MqttPacket> _decodeAll(Uint8List bytes) =>
    MqttPacketDecoder().feed(bytes);

/// Takes everything the client has written and returns the packets of type [T].
List<T> _sent<T extends MqttPacket>(MemoryTransport transport) =>
    _decodeAll(transport.takeOutgoingBytes()).whereType<T>().toList();

void main() {
  group('deferred packet budget', () {
    test('a burst of retained PUBLISHes in the CONNACK segment is delivered',
        () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        transportFactory: () => transport,
        autoReconnect: false,
      );
      final received = <String>[];
      client.messages.listen((m) => received.add(m.topic));

      final connecting = client.connect();
      await _settle(2);
      // One TCP segment carrying CONNACK plus far more packets than the old
      // 128-packet cap allowed.
      transport.inject(_concat([
        _connack(),
        for (var i = 0; i < 500; i++) _publish('retained/$i', 'v'),
      ]));
      await connecting;
      await _settle();

      expect(received, hasLength(500));
      expect(received.first, 'retained/0');
      expect(received.last, 'retained/499');
      expect(client.state, MqttConnectionState.connected);
      await client.close();
    });

    test('a flood large enough to threaten memory is still a protocol error',
        () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        transportFactory: () => transport,
        autoReconnect: false,
      );
      final errors = <Object>[];
      client.errors.listen(errors.add);

      final connecting = client.connect();
      await _settle(2);
      final fat = 'x' * 60000;
      transport.inject(_concat([
        _connack(),
        for (var i = 0; i < 160; i++) _publish('flood/$i', fat),
      ]));
      await connecting.then<void>((_) {}, onError: errors.add);
      await _settle();

      expect(errors.whereType<MqttProtocolException>(), isNotEmpty);
      await client.close();
    });
  });

  group('Maximum Packet Size is per connection (section 3.1.2.11.4)', () {
    test('the previous connection cap does not gate the next CONNECT',
        () async {
      final transports = <MemoryTransport>[];
      final client = MqttClient(
        host: 'h',
        clientId: 'a-deliberately-long-client-identifier-for-this-device-0001',
        autoReconnect: true,
        transportFactory: () {
          final t = MemoryTransport();
          transports.add(t);
          return t;
        },
        reconnectManager: ReconnectManager(
          initialDelay: const Duration(milliseconds: 1),
          maxDelay: const Duration(milliseconds: 1),
          jitterFactor: 0,
        ),
      );
      final errors = <Object>[];
      client.errors.listen(errors.add);

      final connecting = client.connect();
      await _settle(2);
      final connectLength = transports[0].outgoing.first.length;
      // A cap below this client's own CONNECT. It must not survive the
      // connection it was negotiated on.
      transports[0].inject(_connack(
        properties: [MaximumPacketSize(connectLength - 1)],
      ));
      await connecting;
      expect(client.serverCapabilities.maximumPacketSize, connectLength - 1);

      transports[0].injectError(MqttTransportException('peer reset'));
      await _settle(40);

      expect(transports, hasLength(greaterThan(1)));
      expect(
        transports[1].outgoing,
        isNotEmpty,
        reason: 'the reconnect CONNECT must be written',
      );
      expect(errors.whereType<MqttPacketTooLargeException>(), isEmpty);
      await client.close();
    });
  });

  group('CONNACK Session Present (MQTT-3.2.2-6)', () {
    test('a non-zero reason code with Session Present set is rejected', () {
      final bytes = MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: true,
          reasonCode: MqttReasonCode.serverBusy,
        ),
      );
      expect(
        () => MqttPacketCodec.decode(bytes),
        throwsA(isA<MqttProtocolException>()),
      );
    });

    test('Session Present with a success reason code is still accepted', () {
      final bytes = MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: true),
      );
      final decoded = MqttPacketCodec.decode(bytes) as MqttConnackPacket;
      expect(decoded.sessionPresent, isTrue);
    });

    test('a non-zero reason code without Session Present is accepted', () {
      final bytes = MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          reasonCode: MqttReasonCode.serverBusy,
        ),
      );
      final decoded = MqttPacketCodec.decode(bytes) as MqttConnackPacket;
      expect(decoded.reasonCode, MqttReasonCode.serverBusy);
    });
  });

  group('PUBLISH Topic Name (MQTT-3.3.2-2)', () {
    test('a wildcard in the topic name is a protocol error', () {
      for (final topic in ['sensors/+/temp', 'sensors/#', '+', '#']) {
        expect(
          () => MqttPacketCodec.decode(_publish(topic, 'x')),
          throwsA(isA<MqttProtocolException>()),
          reason: topic,
        );
      }
    });

    test('an empty topic name is still accepted, for Topic Alias', () {
      final bytes = MqttPacketCodec.encode(
        MqttPublishPacket(
          topicName: '',
          payload: Uint8List(0),
          properties: const [TopicAlias(4)],
        ),
      );
      final decoded = MqttPacketCodec.decode(bytes) as MqttPublishPacket;
      expect(decoded.topicName, isEmpty);
    });

    test('an ordinary topic name is unaffected', () {
      final decoded =
          MqttPacketCodec.decode(_publish('sensors/1/temp', 'x'))
              as MqttPublishPacket;
      expect(decoded.topicName, 'sensors/1/temp');
    });
  });

  group('connect() argument and property clashes', () {
    test('the same property both ways is an ArgumentError, not a wire error',
        () async {
      final client = MqttClient(
        host: 'h',
        transportFactory: MemoryTransport.new,
        autoReconnect: false,
      );
      await expectLater(
        client.connect(
          maximumPacketSize: 1024,
          properties: const [MaximumPacketSize(2048)],
        ),
        throwsA(isA<ArgumentError>()),
      );
      await expectLater(
        client.connect(
          receiveMaximum: 10,
          properties: const [ReceiveMaximum(20)],
        ),
        throwsA(isA<ArgumentError>()),
      );
      await expectLater(
        client.connect(
          sessionExpiryInterval: const Duration(seconds: 30),
          properties: const [SessionExpiryInterval(60)],
        ),
        throwsA(isA<ArgumentError>()),
      );
      await client.close();
    });

    test('either spelling on its own is still supported', () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        transportFactory: () => transport,
        autoReconnect: false,
      );
      final connecting = client.connect(
        properties: const [ReceiveMaximum(20), MaximumPacketSize(2048)],
      );
      await _settle(2);
      transport.inject(_connack());
      await connecting;
      expect(client.state, MqttConnectionState.connected);
      await client.close();
    });
  });

  group('outgoing Topic Alias supplied by the caller', () {
    test('the mapping is recorded so later publishes reuse it', () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        transportFactory: () => transport,
        autoReconnect: false,
      );
      final connecting = client.connect();
      await _settle(2);
      transport.inject(_connack(properties: const [TopicAliasMaximum(10)]));
      await connecting;
      transport.takeOutgoingBytes();

      final topic = 'telemetry/device/0001/measurements';
      await client.publish(
        topic,
        Uint8List.fromList([1]),
        properties: const [TopicAlias(7)],
      );
      final first =
          MqttPacketCodec.decode(transport.takeOutgoingBytes())
              as MqttPublishPacket;
      // Establishing the alias carries the full topic name alongside it.
      expect(first.topicName, topic);
      expect(
        first.properties.whereType<TopicAlias>().single.value,
        7,
      );

      await client.publish(topic, Uint8List.fromList([2]));
      final second =
          MqttPacketCodec.decode(transport.takeOutgoingBytes())
              as MqttPublishPacket;
      expect(
        second.topicName,
        isEmpty,
        reason: 'the caller-established alias should now stand in for the '
            'topic name',
      );
      expect(
        second.properties.whereType<TopicAlias>().single.value,
        7,
        reason: 'and it must be the alias the caller chose, not a new one',
      );
      await client.close();
    });
  });

  group('MqttTopic.matches (section 4.7)', () {
    test('single-level wildcard', () {
      expect(MqttTopic.matches('sport/+/player1', 'sport/tennis/player1'),
          isTrue);
      expect(MqttTopic.matches('sport/+', 'sport/tennis'), isTrue);
      expect(MqttTopic.matches('sport/+', 'sport'), isFalse);
      expect(MqttTopic.matches('sport/+', 'sport/tennis/player1'), isFalse);
      expect(MqttTopic.matches('+/+', 'sport/tennis'), isTrue);
    });

    test('multi-level wildcard', () {
      expect(MqttTopic.matches('sport/#', 'sport/tennis/player1'), isTrue);
      // MQTT-4.7.1-2: "sport/#" also matches "sport" itself.
      expect(MqttTopic.matches('sport/#', 'sport'), isTrue);
      expect(MqttTopic.matches('#', 'a/b/c'), isTrue);
      expect(MqttTopic.matches('sport/#', 'soccer/x'), isFalse);
    });

    test('exact and non-matching filters', () {
      expect(MqttTopic.matches('a/b', 'a/b'), isTrue);
      expect(MqttTopic.matches('a/b', 'a/b/c'), isFalse);
      expect(MqttTopic.matches('a/b/c', 'a/b'), isFalse);
      expect(MqttTopic.matches('a/b', 'a/B'), isFalse);
    });

    test('a leading wildcard does not match \$-prefixed topics (MQTT-4.7.2-1)',
        () {
      expect(MqttTopic.matches('#', r'$SYS/broker/uptime'), isFalse);
      expect(MqttTopic.matches('+/broker/uptime', r'$SYS/broker/uptime'),
          isFalse);
      // Naming the level literally still matches.
      expect(MqttTopic.matches(r'$SYS/#', r'$SYS/broker/uptime'), isTrue);
      expect(MqttTopic.matches(r'$SYS/+/uptime', r'$SYS/broker/uptime'), isTrue);
    });

    test('a shared subscription matches on its filter part', () {
      expect(
        MqttTopic.matches(r'$share/group1/sport/#', 'sport/tennis'),
        isTrue,
      );
      expect(
        MqttTopic.matches(r'$share/group1/sport/+', 'weather/today'),
        isFalse,
      );
    });

    test('every filter the client accepts can be matched without throwing', () {
      const filters = [
        'a',
        'a/b',
        '+',
        '#',
        'a/+/c',
        'a/#',
        r'$share/g/a/+',
        r'$SYS/#',
      ];
      for (final filter in filters) {
        expect(MqttTopic.checkFilter(filter), isNull, reason: filter);
        expect(() => MqttTopic.matches(filter, 'a/b/c'), returnsNormally);
      }
    });
  });

  group('FlowController fairness', () {
    test('blocked publishes are served in the order they were submitted',
        () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        transportFactory: () => transport,
        autoReconnect: false,
        operationTimeout: Duration.zero,
      );
      final connecting = client.connect();
      await _settle(2);
      // A send quota of exactly one, so every publish after the first queues.
      transport.inject(_connack(properties: const [ReceiveMaximum(1)]));
      await connecting;
      transport.takeOutgoingBytes();

      final completed = <int>[];
      final publishes = <Future<void>>[];
      for (var i = 0; i < 8; i++) {
        publishes.add(
          client
              .publish('t/$i', Uint8List.fromList([i]),
                  qos: MqttQos.atLeastOnce)
              .then((_) => completed.add(i)),
        );
        // Submit in a deterministic order, one event-loop turn apart.
        await _settle(1);
      }

      // Acknowledge them one at a time; the quota of one forces strict
      // hand-off, so the order acks are produced is the order slots are given.
      for (var round = 0; round < 8; round++) {
        await _settle(3);
        final written = transport.takeOutgoingBytes();
        expect(written, isNotEmpty, reason: 'round $round produced no PUBLISH');
        final sent = MqttPacketCodec.decode(written) as MqttPublishPacket;
        expect(
          sent.payload.single,
          round,
          reason: 'publish $round should hold the quota in submission order',
        );
        transport.inject(
          MqttPacketCodec.encode(
            MqttPubackPacket(packetIdentifier: sent.packetIdentifier),
          ),
        );
      }

      await Future.wait(publishes);
      expect(completed, List.generate(8, (i) => i));
      await client.close();
    });

    test('a timed-out waiter does not strand the slot it never took', () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        transportFactory: () => transport,
        autoReconnect: false,
        operationTimeout: const Duration(milliseconds: 60),
      );
      final connecting = client.connect();
      await _settle(2);
      transport.inject(_connack(properties: const [ReceiveMaximum(1)]));
      await connecting;
      transport.takeOutgoingBytes();

      // Both will time out; attach the expectations before either can reject,
      // so neither becomes an unhandled asynchronous error.
      final first = expectLater(
        client.publish('t/1', Uint8List.fromList([1]),
            qos: MqttQos.atLeastOnce),
        throwsA(isA<MqttTimeoutException>()),
      );
      await _settle(2);
      final blocked = expectLater(
        client.publish('t/2', Uint8List.fromList([2]),
            qos: MqttQos.atLeastOnce),
        throwsA(isA<MqttTimeoutException>()),
      );
      await Future.wait([first, blocked]);

      // The publish that timed out keeps its slot on purpose: it stays in the
      // session store for retransmission. The waiter that gave up is the one
      // that must be gone from the queue — otherwise the slot freed by the
      // late PUBACK below would be handed to a caller nobody is awaiting, and
      // the quota would be stranded for the life of the connection.
      final stillInflight = _sent<MqttPublishPacket>(transport).single;
      transport.inject(MqttPacketCodec.encode(
        MqttPubackPacket(packetIdentifier: stillInflight.packetIdentifier),
      ));
      await _settle(3);

      final recovered = client.publish('t/3', Uint8List.fromList([3]),
          qos: MqttQos.atLeastOnce);
      await _settle(3);
      final publishes = _sent<MqttPublishPacket>(transport);
      expect(
        publishes,
        hasLength(1),
        reason: 'the slot the late PUBACK freed must be usable again',
      );
      transport.inject(MqttPacketCodec.encode(
        MqttPubackPacket(packetIdentifier: publishes.single.packetIdentifier),
      ));
      await recovered;
      await client.close();
    });
  });

  group('pingResponseTimeout', () {
    test('a dead link is detected without waiting a second keep alive',
        () async {
      final transports = <MemoryTransport>[];
      final client = MqttClient(
        host: 'h',
        autoReconnect: false,
        // Keep alive stays long; only the answer window is short.
        pingResponseTimeout: const Duration(milliseconds: 40),
        transportFactory: () {
          final t = MemoryTransport();
          transports.add(t);
          return t;
        },
      );
      final errors = <Object>[];
      client.errors.listen(errors.add);

      final connecting = client.connect(
        keepAlive: const Duration(seconds: 1),
      );
      await _settle(2);
      transports[0].inject(_connack());
      await connecting;
      transports[0].takeOutgoingBytes();

      // Idle past the keep alive so a PINGREQ goes out, then never answer it.
      await Future<void>.delayed(const Duration(milliseconds: 1100));
      expect(
        _sent<MqttPingreqPacket>(transports[0]),
        hasLength(1),
        reason: 'the idle timer should have sent a PINGREQ',
      );

      // With the default the client would wait another full second; 40 ms in,
      // it should already have given up.
      await Future<void>.delayed(const Duration(milliseconds: 120));
      expect(client.state, MqttConnectionState.disconnected);
      expect(
        errors.whereType<MqttConnectionException>().map((e) => e.message),
        contains(contains('PINGRESP')),
      );
      await client.close();
    });

    test('an answered PINGREQ keeps the connection up', () async {
      final transport = MemoryTransport();
      final client = MqttClient(
        host: 'h',
        autoReconnect: false,
        pingResponseTimeout: const Duration(milliseconds: 200),
        transportFactory: () => transport,
      );
      final connecting = client.connect(
        keepAlive: const Duration(seconds: 1),
      );
      await _settle(2);
      transport.inject(_connack());
      await connecting;
      transport.takeOutgoingBytes();

      await Future<void>.delayed(const Duration(milliseconds: 1100));
      expect(_sent<MqttPingreqPacket>(transport), hasLength(1));
      transport.inject(
        MqttPacketCodec.encode(const MqttPingrespPacket()),
      );
      await Future<void>.delayed(const Duration(milliseconds: 300));

      expect(client.state, MqttConnectionState.connected);
      expect(client.metrics.lastPingRtt, isNotNull);
      await client.close();
    });

    test('a non-positive value is rejected at construction', () {
      expect(
        () => MqttClient(host: 'h', pingResponseTimeout: Duration.zero),
        throwsA(isA<ArgumentError>()),
      );
    });
  });

  group('re-subscribe keeps its packet identifier on timeout', () {
    test('a timed-out re-subscribe does not release the identifier', () async {
      final transports = <MemoryTransport>[];
      final client = MqttClient(
        host: 'h',
        autoReconnect: true,
        operationTimeout: const Duration(milliseconds: 60),
        transportFactory: () {
          final t = MemoryTransport();
          transports.add(t);
          return t;
        },
        reconnectManager: ReconnectManager(
          initialDelay: const Duration(milliseconds: 1),
          maxDelay: const Duration(milliseconds: 1),
          jitterFactor: 0,
        ),
      );
      final connecting = client.connect(cleanStart: false);
      await _settle(2);
      transports[0].inject(_connack());
      await connecting;

      // Establish a subscription, so losing the session triggers a re-subscribe.
      transports[0].takeOutgoingBytes();
      final subscribing = client.subscribe('sensors/#');
      await _settle(2);
      final subscribe = _sent<MqttSubscribePacket>(transports[0]).single;
      transports[0].inject(MqttPacketCodec.encode(MqttSubackPacket(
        packetIdentifier: subscribe.packetIdentifier,
        reasonCodes: const [0x00],
      )));
      await subscribing;

      // Drop the link. The new connection reports Session Present 0, so the
      // client re-subscribes; never answer that SUBSCRIBE, so it times out.
      transports[0].injectError(MqttTransportException('peer reset'));
      await _settle(30);
      expect(transports, hasLength(greaterThan(1)));
      transports[1].takeOutgoingBytes();
      transports[1].inject(_connack());
      await _settle(10);

      final resubscribe = _sent<MqttSubscribePacket>(transports[1]).single;
      expect(resubscribe.subscriptions.single.topicFilter, 'sensors/#');

      await Future<void>.delayed(const Duration(milliseconds: 120));
      await _settle();

      // MQTT-2.2.1-4: the identifier stays in use until its SUBACK is
      // processed, so the next allocation must not hand it out again.
      final other = client.subscribe('other/#');
      await _settle(4);
      final next = _sent<MqttSubscribePacket>(transports[1]).single;
      expect(
        next.packetIdentifier,
        isNot(resubscribe.packetIdentifier),
        reason: 'the timed-out re-subscribe still owns its identifier',
      );

      transports[1].inject(MqttPacketCodec.encode(MqttSubackPacket(
        packetIdentifier: next.packetIdentifier,
        reasonCodes: const [0x00],
      )));
      await other;
      expect(client.state, MqttConnectionState.connected);
      await client.close();
    });
  });
}
