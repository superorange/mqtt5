/// Public API surface that the protocol tests do not reach on their own:
/// argument validation, the codec used in the server role (checked against
/// real mosquitto clients), topic matching (checked against mosquitto's own
/// routing), and broker capability switches (EMQX). No mocks: every peer is
/// a real MQTT implementation.
@Tags(['real-broker'])
@Timeout(Duration(seconds: 60))
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'support/common.dart';
import 'support/emqx.dart';

/// A minimal MQTT 5 server built only from the library's exported codec.
final class CodecServer {
  CodecServer._(this._server);
  final ServerSocket _server;
  final List<MqttPacket> received = [];
  final _events = StreamController<MqttPacket>.broadcast();
  Socket? client;
  int get port => _server.port;

  static Future<CodecServer> start() async {
    final s =
        CodecServer._(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));
    s._server.listen(s._accept);
    return s;
  }

  void send(MqttPacket p) => client!.add(MqttPacketCodec.encode(p));

  Future<T> next<T extends MqttPacket>() async {
    final deadline = DateTime.now().add(const Duration(seconds: 10));
    while (true) {
      for (final p in received) {
        if (p is T) return p;
      }
      if (DateTime.now().isAfter(deadline)) {
        throw TimeoutException('no $T; got $received');
      }
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
  }

  void _accept(Socket socket) {
    client = socket;
    socket.done.then((_) {}, onError: (_) {});
    final decoder = MqttPacketDecoder();
    socket.listen((data) {
      for (final p in decoder.feed(data)) {
        received.add(p);
        _events.add(p);
        _respond(p);
      }
    }, onError: (_) {});
  }

  void _respond(MqttPacket p) {
    switch (p) {
      case MqttConnectPacket():
        send(const MqttConnackPacket(sessionPresent: false, properties: [
          ReceiveMaximum(5),
          TopicAliasMaximum(3),
          ReasonString('welcome'),
          UserProperty('srv', '1'),
        ]));
      case MqttSubscribePacket(:final packetIdentifier, :final subscriptions):
        send(MqttSubackPacket(
          packetIdentifier: packetIdentifier,
          reasonCodes: [
            for (final s in subscriptions)
              s.topicFilter == 'deny' ? 0x87 : s.options.qos.value
          ],
          properties: const [ReasonString('ok')],
        ));
      case MqttUnsubscribePacket(:final packetIdentifier, :final topicFilters):
        send(MqttUnsubackPacket(
          packetIdentifier: packetIdentifier,
          reasonCodes: [for (final _ in topicFilters) 0x11],
        ));
      case MqttPingreqPacket():
        send(const MqttPingrespPacket());
      case MqttPublishPacket(:final qos, :final packetIdentifier):
        if (qos == MqttQos.atLeastOnce) {
          send(MqttPubackPacket(packetIdentifier: packetIdentifier));
        } else if (qos == MqttQos.exactlyOnce) {
          send(MqttPubrecPacket(packetIdentifier: packetIdentifier));
        }
      case MqttPubrelPacket(:final packetIdentifier):
        send(MqttPubcompPacket(packetIdentifier: packetIdentifier));
      case MqttPubrecPacket(:final packetIdentifier):
        send(MqttPubrelPacket(packetIdentifier: packetIdentifier));
      default:
        break;
    }
  }

  Future<void> close() async {
    client?.destroy();
    await _server.close();
    await _events.close();
  }
}

void main() {
  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
    return;
  }

  group('codec in the server role, against real mosquitto clients', () {
    late CodecServer server;
    setUp(() async => server = await CodecServer.start());
    tearDown(() => server.close());

    test(
        'mosquitto_sub: CONNECT (will, credentials, properties), SUBSCRIBE, '
        'UNSUBSCRIBE, PINGREQ decoded; CONNACK/SUBACK/UNSUBACK/PINGRESP and '
        'QoS 1 + QoS 2 PUBLISH accepted', () async {
      final sub = await Process.start('script', [
        '-q',
        '/dev/null',
        requireMosquittoProgram('mosquitto_sub'),
        '-V',
        'mqttv5',
        '-h',
        '127.0.0.1',
        '-p',
        '${server.port}',
        '-i',
        'interop-sub',
        '-k',
        '5',
        '-u',
        'user',
        '-P',
        'pa:ss',
        '-c',
        '-x',
        '60',
        '-q',
        '2',
        '-t',
        'a/#',
        '-t',
        'deny',
        '-U',
        'gone/+',
        '--will-topic',
        'w/t',
        '--will-payload',
        'bye',
        '--will-qos',
        '1',
        '--will-retain',
        '-D',
        'connect',
        'user-property',
        'ck',
        'cv',
        '-D',
        'connect',
        'receive-maximum',
        '10',
        '-D',
        'will',
        'content-type',
        'text/x',
        '-D',
        'will',
        'will-delay-interval',
        '5',
        '-D',
        'subscribe',
        'subscription-identifier',
        '7',
        '-d',
        '-F',
        '%j',
      ]);
      final out = StringBuffer();
      sub.stdout.transform(utf8.decoder).listen(out.write);
      sub.stderr.transform(utf8.decoder).listen(out.write);
      try {
        final connect = await server.next<MqttConnectPacket>();
        expect(connect.clientId, 'interop-sub');
        expect(connect.keepAliveSeconds, 5);
        expect(connect.cleanStart, isFalse);
        expect(connect.username, 'user');
        expect(utf8.decode(connect.password!), 'pa:ss');
        expect(
            connect.properties
                .whereType<SessionExpiryInterval>()
                .single
                .seconds,
            60);
        expect(connect.properties.whereType<ReceiveMaximum>().single.value, 10);
        expect(connect.properties.whereType<UserProperty>().single.value, 'cv');
        final will = connect.will!;
        expect(will.topic, 'w/t');
        expect(utf8.decode(will.payload), 'bye');
        expect(will.qos, MqttQos.atLeastOnce);
        expect(will.retain, isTrue);
        expect(will.properties.whereType<ContentType>().single.value, 'text/x');
        expect(
            will.properties.whereType<WillDelayInterval>().single.seconds, 5);

        final subscribe = await server.next<MqttSubscribePacket>();
        expect(
            subscribe.subscriptions.map((s) => s.topicFilter), ['a/#', 'deny']);
        expect(subscribe.subscriptions.first.options,
            const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
        expect('${subscribe.subscriptions.first.options}', contains('qos'));
        expect(
            subscribe.subscriptions.first,
            MqttSubscription('a/#',
                options: subscribe.subscriptions.first.options));
        expect(
            subscribe.subscriptions.first.hashCode,
            MqttSubscription('a/#',
                    options: subscribe.subscriptions.first.options)
                .hashCode);
        expect(
            subscribe.properties
                .whereType<SubscriptionIdentifier>()
                .single
                .value,
            7);
        final unsubscribe = await server.next<MqttUnsubscribePacket>();
        expect(unsubscribe.topicFilters, ['gone/+']);
        await server
            .next<MqttPingreqPacket>()
            .timeout(const Duration(seconds: 10));

        server.send(MqttPublishPacket(
          topicName: 'a/x',
          payload: bytes('q1'),
          qos: MqttQos.atLeastOnce,
          packetIdentifier: 1,
          properties: const [
            ContentType('text/plain'),
            SubscriptionIdentifier(7)
          ],
        ));
        server.send(MqttPublishPacket(
          topicName: 'a/y',
          payload: bytes('q2'),
          qos: MqttQos.exactlyOnce,
          packetIdentifier: 2,
        ));
        final puback = await server.next<MqttPubackPacket>();
        expect(puback.packetIdentifier, 1);
        await server.next<MqttPubrecPacket>();
        await server.next<MqttPubcompPacket>();
        await waitUntil(() => out.toString().contains('"payload":"q2"'));
        final text = out.toString();
        expect(text, contains('received CONNACK (0)'));
        expect(text, contains('Subscribed (mid: 1): 2, 135'));
        expect(text, contains('received UNSUBACK'));
        expect(text, contains('received PINGRESP'));
        expect(text, contains('"content-type":"text/plain"'));
        expect(text, contains('"payload":"q1"'));
      } finally {
        sub.kill();
        await sub.exitCode;
        printOnFailure(out.toString());
      }
    });

    test(
        'mosquitto_pub: QoS 2 PUBLISH with properties and a topic alias is '
        'decoded; the exchange completes and the client exits cleanly',
        () async {
      final r = await Process.run(requireMosquittoProgram('mosquitto_pub'), [
        '-V',
        'mqttv5',
        '-h',
        '127.0.0.1',
        '-p',
        '${server.port}',
        '-i',
        'interop-pub',
        '-q',
        '2',
        '-t',
        'p/t',
        '-m',
        'hello',
        '-D',
        'publish',
        'topic-alias',
        '1',
        '-D',
        'publish',
        'content-type',
        'text/plain',
        '-D',
        'publish',
        'message-expiry-interval',
        '30',
        '-D',
        'publish',
        'payload-format-indicator',
        '1',
        '-D',
        'publish',
        'response-topic',
        'r/t',
        '-D',
        'publish',
        'correlation-data',
        'cd',
        '-D',
        'publish',
        'user-property',
        'pk',
        'pv',
      ]);
      expect(r.exitCode, 0, reason: '${r.stdout}${r.stderr}');
      final publish = await server.next<MqttPublishPacket>();
      expect(publish.topicName, 'p/t');
      expect(publish.qos, MqttQos.exactlyOnce);
      expect(utf8.decode(publish.payload), 'hello');
      final p = publish.properties;
      expect(p.whereType<TopicAlias>().single.value, 1);
      expect(p.whereType<ContentType>().single.value, 'text/plain');
      expect(p.whereType<MessageExpiryInterval>().single.seconds, 30);
      expect(p.whereType<PayloadFormatIndicator>().single.value, 1);
      expect(p.whereType<ResponseTopic>().single.value, 'r/t');
      expect(p.whereType<CorrelationData>().single,
          CorrelationData(utf8.encode('cd')));
      expect(
          p.whereType<UserProperty>().single, const UserProperty('pk', 'pv'));
      expect('$publish', 'publish');
    });
  });

  group('MqttTopic.matches agrees with mosquitto routing', () {
    test('differential check over filters x topics', () async {
      final broker = await Mosquitto.start(config: ['sys_interval 1']);
      addTearDown(broker.dispose);
      const filters = [
        '#',
        '+',
        '+/+',
        '+/#',
        'a',
        'a/#',
        'a/+',
        'a/+/c',
        'a/b/c',
        'a//c',
        '/+',
        '+/',
        '/#',
        'a/+/+/#',
        r'$share/g/a/+',
        r'$SYS/#',
        r'+/broker/#',
      ];
      const topics = [
        'a',
        'a/b',
        'a/b/c',
        'a//c',
        '/a',
        'a/',
        'b/c',
        'a/b/c/d',
        '/',
        'x',
      ];
      final c = newClient(broker.port, clientId: 'match');
      await c.connect();
      final inbox = Inbox(c);
      for (var i = 0; i < filters.length; i++) {
        await c.subscribe(filters[i], subscriptionIdentifier: i + 1);
      }
      for (final t in topics) {
        await c.publish(t, bytes(t));
      }
      await settle(1500);
      final routed = <String, Set<int>>{};
      for (final m in inbox.messages) {
        routed.putIfAbsent(m.topic, () => {}).addAll(m.subscriptionIdentifiers);
      }
      expect(routed.keys, contains(r'$SYS/broker/version'));
      for (final t in [...topics, r'$SYS/broker/version']) {
        for (var i = 0; i < filters.length; i++) {
          expect(MqttTopic.matches(filters[i], t),
              routed[t]?.contains(i + 1) ?? false,
              reason: 'filter "${filters[i]}" vs topic "$t"');
        }
      }
      await c.close();
    });
  });

  group('argument validation (fails before anything is sent)', () {
    late Mosquitto broker;
    setUpAll(() async => broker = await Mosquitto.start());
    tearDownAll(() => broker.dispose());

    test('constructor arguments', () {
      MqttClient make({
        int port = 1883,
        Duration connectionTimeout = const Duration(seconds: 1),
        Duration operationTimeout = Duration.zero,
        Duration? pingResponseTimeout,
      }) =>
          MqttClient(
            host: 'h',
            port: port,
            connectionTimeout: connectionTimeout,
            operationTimeout: operationTimeout,
            pingResponseTimeout: pingResponseTimeout,
          );
      expect(() => make(port: 0), throwsArgumentError);
      expect(() => make(port: 70000), throwsArgumentError);
      expect(() => make(connectionTimeout: Duration.zero), throwsArgumentError);
      expect(() => make(operationTimeout: const Duration(seconds: -1)),
          throwsArgumentError);
      expect(
          () => make(pingResponseTimeout: Duration.zero), throwsArgumentError);
      expect(make().clientId, startsWith('mqtt5-'));
      expect(() => ReconnectManager(initialDelay: const Duration(seconds: -1)),
          throwsArgumentError);
      expect(
          () => ReconnectManager(
              initialDelay: const Duration(seconds: 2),
              maxDelay: const Duration(seconds: 1)),
          throwsArgumentError);
      expect(() => ReconnectManager(jitterFactor: 2), throwsArgumentError);
      expect(() => ReconnectManager(stableAfter: const Duration(seconds: -1)),
          throwsArgumentError);
      expect(() => ReconnectManager(flapWindow: const Duration(seconds: -1)),
          throwsArgumentError);
      final backoff = ReconnectManager(
          initialDelay: const Duration(milliseconds: 100),
          maxDelay: const Duration(milliseconds: 250),
          jitterFactor: 0);
      expect([for (var i = 0; i < 4; i++) backoff.nextDelay().inMilliseconds],
          [100, 200, 250, 250]);
    });

    test('connect() arguments', () async {
      final c = newClient(broker.port, clientId: 'args');
      Future<void> bad(Future<void> Function() f) =>
          expectLater(f(), throwsArgumentError);
      await bad(() => c.connect(keepAlive: const Duration(seconds: -1)));
      await bad(() => c.connect(keepAlive: const Duration(seconds: 65536)));
      await bad(() => c.connect(receiveMaximum: 0));
      await bad(() => c.connect(maximumPacketSize: 0));
      await bad(() => c.connect(topicAliasMaximum: 65536));
      await bad(
          () => c.connect(sessionExpiryInterval: const Duration(seconds: -1)));
      await bad(() => c.connect(connackTimeout: Duration.zero));
      await bad(() => c.connect(authenticationData: Uint8List(1)));
      await bad(() => c.connect(
          authenticationMethod: 'm', authenticationData: Uint8List(70000)));
      await bad(() => c.connect(
          sessionExpiryInterval: const Duration(seconds: 1),
          properties: const [SessionExpiryInterval(1)]));
      expect(c.state, MqttConnectionState.disconnected);
      expect(broker.countLog('as args'), 0);
      await c.close();
    });

    test('operation arguments on a live connection', () async {
      final proxy = await FaultProxy.start(broker.port);
      addTearDown(proxy.close);
      final c = newClient(proxy.port, clientId: 'ops');
      await c.connect(properties: const [SessionExpiryInterval(0)]);
      // connect() while connected joins when every setting matches.
      await c.connect(properties: const [SessionExpiryInterval(0)]);
      expect(proxy.sent(kConnect), hasLength(1));
      expect(() => c.publish('a/+', bytes('x')), throwsArgumentError);
      expect(() => c.publish('', bytes('x')), throwsArgumentError);
      // Unpaired surrogates cannot be encoded as UTF-8 (MQTT-1.5.4-1).
      expect(() => c.publish('a/\uD800', bytes('x')), throwsArgumentError);
      expect(
          () => c.publish('a/b', bytes('x'),
              properties: const [UserProperty('k', '\uDC00')]),
          throwsArgumentError);
      expect(() => c.subscribe('a/#/b'), throwsArgumentError);
      expect(() => c.subscribe('a+'), throwsArgumentError);
      expect(() => c.subscribe(r'$share/g'), throwsArgumentError);
      expect(() => c.subscribe(r'$share//t'), throwsArgumentError);
      expect(() => c.subscribe(r'$share/g+/t'), throwsArgumentError);
      expect(() => c.subscribe('x' * 70000), throwsArgumentError);
      expect(
          () => c.subscribe(r'$share/g/t',
              options: const MqttSubscriptionOptions(noLocal: true)),
          throwsArgumentError);
      expect(() => c.subscribe('t', subscriptionIdentifier: 0),
          throwsArgumentError);
      expect(() => c.unsubscribe(['a/#/b']), throwsArgumentError);
      expect(() => c.reauthenticate(authenticationData: Uint8List(70000)),
          throwsArgumentError);
      // SessionExpiryInterval(0) arrived through `properties`, so a non-zero
      // value on DISCONNECT is refused.
      expect(() => c.disconnect(properties: const [SessionExpiryInterval(5)]),
          throwsArgumentError);
      await c.subscribeAll(const []);
      await c.unsubscribe(const []);
      await settle(200);
      expect(proxy.sent(kSubscribe), isEmpty);
      expect(proxy.sent(kPublish), isEmpty);
      expect(c.state, MqttConnectionState.connected);
      await c.close();
      // Closed for good.
      await c.close();
      await expectLater(c.connect(), throwsA(isA<MqttConnectionException>()));
      expect(() => c.publish('t', bytes('x')),
          throwsA(isA<MqttConnectionException>()));
    });

    test('packet and property constructors reject unencodable values', () {
      expect(() => MqttConnectPacket(clientId: 'c', keepAliveSeconds: 70000),
          throwsArgumentError);
      expect(
          () => MqttConnectPacket(
              clientId: 'c',
              will: MqttWill(topic: 't', payload: Uint8List(70000))),
          throwsArgumentError);
      expect(() => MqttConnectPacket(clientId: 'c', password: Uint8List(70000)),
          throwsArgumentError);
      expect(
          () => MqttPublishPacket(
              topicName: 't', payload: Uint8List(0), qos: MqttQos.atLeastOnce),
          throwsArgumentError);
      expect(
          () => MqttPublishPacket(
              topicName: 't', payload: Uint8List(0), dup: true),
          throwsArgumentError);
      expect(() => MqttSubscribePacket(packetIdentifier: 1, subscriptions: []),
          throwsArgumentError);
      expect(() => MqttUnsubscribePacket(packetIdentifier: 1, topicFilters: []),
          throwsArgumentError);
      expect(() => MqttSubackPacket(packetIdentifier: 1, reasonCodes: []),
          throwsArgumentError);
      expect(() => MqttUnsubackPacket(packetIdentifier: 1, reasonCodes: []),
          throwsArgumentError);
      expect(
          () => MqttPacketCodec.encode(
              MqttPublishPacket(topicName: 'x' * 70000, payload: Uint8List(0))),
          throwsArgumentError);
      expect(
          () => MqttPacketCodec.encode(
              MqttPublishPacket(topicName: 'a\u0000b', payload: Uint8List(0))),
          throwsArgumentError);
      expect(
          () => MqttPacketCodec.encode(const MqttConnackPacket(
              sessionPresent: false, properties: [ReceiveMaximum(70000)])),
          throwsArgumentError);
      expect(
          () => MqttPacketCodec.encode(const MqttConnackPacket(
              sessionPresent: false,
              properties: [MaximumPacketSize(0x100000000)])),
          throwsArgumentError);
      expect(
          () => MqttPacketCodec.encode(const MqttConnackPacket(
              sessionPresent: false,
              properties: [ReceiveMaximum(1), ReceiveMaximum(2)])),
          throwsA(isA<MqttProtocolException>()));
      expect(
          () => MqttPacketCodec.encode(const MqttConnackPacket(
              sessionPresent: false, properties: [TopicAlias(1)])),
          throwsA(isA<MqttProtocolException>()));
      expect(
          () => MqttPacketCodec.encode(MqttPublishPacket(
              topicName: 't',
              payload: Uint8List(0),
              properties: const [SubscriptionIdentifier(268435456)])),
          throwsA(isA<RangeError>()));
      expect(() => TlsTransport.createSecurityContext(certificateChain: 'x'),
          throwsArgumentError);
      expect(() => TlsTransport.createSecurityContext(keyPassword: 'x'),
          throwsArgumentError);
    });

    test('property value semantics', () {
      expect(CorrelationData([1, 2]), CorrelationData([1, 2]));
      expect(
          CorrelationData([1, 2]).hashCode, CorrelationData([1, 2]).hashCode);
      expect(CorrelationData([1, 2]) == CorrelationData([1, 3]), isFalse);
      expect(CorrelationData([1]) == CorrelationData([1, 2]), isFalse);
      expect(const ContentType('a') == const ResponseTopic('a'), isFalse);
      expect(const ContentType('a') == Object(), isFalse);
      expect(const UserProperty('k', 'v'), const UserProperty('k', 'v'));
      expect('${const ContentType('a')}', 'Content Type(a)');
      expect(const ResponseInformation('r').wireValue, 'r');
      expect(const AssignedClientIdentifier('c').wireValue, 'c');
      expect(const ServerKeepAlive(3).wireValue, 3);
    });

    test('PrintLogger filters by level; metrics render human-readable sizes',
        () async {
      final printed = <String>[];
      await runZoned(() async {
        final logger = PrintLogger(minimumLevel: MqttLogLevel.warning);
        logger.log(MqttLogLevel.info, 'hidden');
        logger.log(MqttLogLevel.error, 'shown');
        logger.log(MqttLogLevel.none, 'never');
        PrintLogger(minimumLevel: MqttLogLevel.none)
            .log(MqttLogLevel.error, 'never');
        final c = newClient(broker.port, clientId: 'metrics', logger: logger);
        await c.connect();
        await c.publish('m/t', Uint8List(600 * 1024), qos: MqttQos.atLeastOnce);
        expect('${c.metrics}', contains('KB'));
        await c.publish('m/t', Uint8List(600 * 1024), qos: MqttQos.atLeastOnce);
        expect('${c.metrics}', contains('MB'));
        expect('${c.metrics}', contains(' B'));
        await c.close();
      },
          zoneSpecification: ZoneSpecification(
              print: (_, __, ___, line) => printed.add(line)));
      expect(printed, contains('mqtt5 [error] shown'));
      expect(printed.where((l) => l.contains('hidden') || l.contains('never')),
          isEmpty);
    });

    test('custom transportFactory, reconnectCleanStart and errorEvents',
        () async {
      final proxy = await FaultProxy.start(broker.port);
      addTearDown(proxy.close);
      final c = MqttClient(
        host: 'unused',
        clientId: 'factory',
        transportFactory: () => TcpTransport(
            host: '127.0.0.1', port: proxy.port, sourceAddress: '127.0.0.1'),
        reconnectManager: fastReconnect(),
      );
      await c.connect(
          cleanStart: false,
          reconnectCleanStart: true,
          sessionExpiryInterval: const Duration(seconds: 60));
      expect(proxy.sent(kConnect).single.connect.flags & 0x02, 0);
      proxy.cutAll();
      await waitUntil(() =>
          proxy.sent(kConnect).length == 2 &&
          c.state == MqttConnectionState.connected);
      expect(proxy.sent(kConnect).last.connect.flags & 0x02, 0x02,
          reason: 'reconnectCleanStart: true');
      await c.close();

      final events = <MqttErrorEvent>[];
      final d = newClient(proxy.port, clientId: 'events', autoReconnect: false);
      d.errorEvents.listen(events.add);
      await d.connect();
      proxy.cutAll();
      await waitUntil(() => events.isNotEmpty);
      expect(events.single.error, isA<MqttException>());
      await d.close();
    });

    test('a manually supplied Topic Alias is bound and reused', () async {
      final b2 = await Mosquitto.start(config: ['max_topic_alias 4']);
      addTearDown(b2.dispose);
      final proxy = await FaultProxy.start(b2.port);
      addTearDown(proxy.close);
      final witness = await MosqSub.start(b2.port, ['ma/#']);
      final c = newClient(proxy.port, clientId: 'manual-alias');
      await c.connect();
      await expectLater(
          c.publish('ma/t', bytes('x'), properties: const [TopicAlias(5)]),
          throwsA(isA<MqttFlowControlException>()));
      await c.publish('ma/t', bytes('1'), properties: const [TopicAlias(4)]);
      await c.publish('ma/t', bytes('2'));
      await witness.waitFor(2);
      expect(witness.messages.map((m) => m['topic']), ['ma/t', 'ma/t']);
      final pubs = proxy.sent(kPublish).map((f) => f.publish).toList();
      expect(pubs[1].topic, '');
      expect(prop(pubs[1].properties, 0x23), 4);
      await witness.stop();
      await c.close();
    });

    test(
        'CONNACK-only properties: Subscription Identifier Available = 0 is '
        'enforced; Response Information and a granted Session Expiry are '
        'reported', () async {
      final proxy = await FaultProxy.start(broker.port);
      addTearDown(proxy.close);
      proxy.rewrite = (f) => f.type == kConnack
          ? [
              Uint8List.fromList(rawPacket(0x20, [
                0,
                0,
                ...rawProps([
                  0x29,
                  0,
                  0x1A,
                  ...rawString('resp/x'),
                  0x11,
                  0,
                  0,
                  0,
                  30,
                ]),
              ]))
            ]
          : null;
      final c = newClient(proxy.port, clientId: 'nosid');
      await c.connect(
          sessionExpiryInterval: const Duration(seconds: 600),
          properties: const [RequestResponseInformation(1)]);
      expect(c.serverCapabilities.subscriptionIdentifierAvailable, isFalse);
      expect(c.responseInformation, 'resp/x');
      expect(c.serverCapabilities.sessionExpiryInterval,
          const Duration(seconds: 30));
      await expectLater(c.subscribe('t', subscriptionIdentifier: 1),
          throwsA(isA<MqttFlowControlException>()));
      await c.close();
    });
  });

  group('EMQX capability switches', () {
    late Emqx emqx;
    var available = false;
    setUpAll(() async {
      available = await Emqx.available;
      if (!available) return;
      emqx = await Emqx.start(env: {
        'EMQX_MQTT__SHARED_SUBSCRIPTION': 'false',
        'EMQX_MQTT__WILDCARD_SUBSCRIPTION': 'false',
        // Response Information is not configured: EMQX 5.8.6 crashes while
        // serialising it into CONNACK (badarg in
        // emqx_frame:serialize_utf8_string), quoted or not.
        'EMQX_MQTT__MAX_SUBSCRIPTIONS': '2',
      });
    });
    tearDownAll(() async {
      if (available) await emqx.dispose();
    });

    test(
        'unsupported shared/wildcard subscriptions are refused locally; '
        'subscription quota surfaces 0x97', () async {
      if (!available) return markTestSkipped('EMQX image missing');
      final c = newClient(emqx.port, clientId: 'emqx-caps');
      await c.connect();
      final caps = c.serverCapabilities;
      expect(caps.sharedSubscriptionAvailable, isFalse);
      expect(caps.wildcardSubscriptionAvailable, isFalse);
      await expectLater(
          c.subscribe(r'$share/g/t'), throwsA(isA<MqttFlowControlException>()));
      await expectLater(
          c.subscribe('a/+'), throwsA(isA<MqttFlowControlException>()));
      await c.subscribe('q/1');
      await c.subscribe('q/2');
      await expectLater(
          c.subscribe('q/3'),
          throwsA(isA<MqttServerRejectedException>()
              .having((e) => e.reasonCode, 'rc', 0x97)));
      expect(c.state, MqttConnectionState.connected);
      await c.close();
    });
  });
}
