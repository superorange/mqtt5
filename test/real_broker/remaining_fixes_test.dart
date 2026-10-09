/// Cross-checks for the remaining behavior fixes, against mosquitto, EMQX,
/// and a raw TCP peer for packets a real broker will not send.
@Tags(['real-broker'])
@Timeout(Duration(seconds: 120))
library;

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'support/common.dart';
import 'support/emqx.dart';

const _sei = Duration(seconds: 60);

Future<void> main() async {
  group('raw peer', () {
    late _RawPeer peer;

    setUp(() async {
      peer = await _RawPeer.start();
    });
    tearDown(() => peer.close());

    test('CONNACK and an unknown alias in one read fail connect', () async {
      final client = _client(peer.port, 'raw-alias');
      final connecting = client.connect(
        cleanStart: true,
        topicAliasMaximum: 10,
      );
      await peer.connected;
      peer.send([
        const MqttConnackPacket(sessionPresent: false),
        MqttPublishPacket(
          topicName: '',
          payload: Uint8List(0),
          packetIdentifier: 1,
          qos: MqttQos.atLeastOnce,
          properties: const [TopicAlias(1)],
        ),
      ]);
      try {
        await connecting;
        fail('connect() should reject an unknown topic alias');
      } on MqttTopicAliasInvalidException {
        fail('an in-range unknown alias is 0x82, not 0x94');
      } on MqttProtocolException {
        // Expected.
      }
      expect(client.state, MqttConnectionState.disconnected);
      // The DISCONNECT is flushed before the socket closes. The peer's read
      // callback runs after this future, so wait for the packet.
      final disconnect = await peer.waitFor<MqttDisconnectPacket>();
      expect(disconnect.reasonCode, MqttReasonCode.protocolError);
      await client.close();
    });

    test('an alias above the client maximum is 0x94', () async {
      final client = _client(peer.port, 'raw-max');
      final connecting = client.connect(
        cleanStart: true,
        topicAliasMaximum: 2,
      );
      await peer.connected;
      peer.send([
        const MqttConnackPacket(sessionPresent: false),
        MqttPublishPacket(
          topicName: '',
          payload: Uint8List(0),
          packetIdentifier: 1,
          qos: MqttQos.atLeastOnce,
          properties: const [TopicAlias(3)],
        ),
      ]);
      await expectLater(
        connecting,
        throwsA(isA<MqttTopicAliasInvalidException>()),
      );
      final disconnect = await peer.waitFor<MqttDisconnectPacket>();
      expect(disconnect.reasonCode, MqttReasonCode.topicAliasInvalid);
      await client.close();
    });

    test('a short SUBACK disconnects', () async {
      final client = _client(peer.port, 'raw-sub');
      final connecting = client.connect(cleanStart: true);
      await peer.connected;
      peer.send([const MqttConnackPacket(sessionPresent: false)]);
      await connecting;

      final subscribing = client.subscribeAll(const [
        MqttSubscription('raw/one'),
        MqttSubscription('raw/two'),
      ]);
      final subscribe = await peer.waitFor<MqttSubscribePacket>();
      peer.send([
        MqttSubackPacket(
          packetIdentifier: subscribe.packetIdentifier,
          reasonCodes: const [0x00],
        ),
      ]);
      await expectLater(subscribing, throwsA(isA<MqttProtocolException>()));
      await _until(() => client.state != MqttConnectionState.connected);
      expect(
        peer.packets.whereType<MqttDisconnectPacket>().single.reasonCode,
        MqttReasonCode.protocolError,
      );
      await client.close();
    });
  });

  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
  } else {
    group('mosquitto', () {
      late Mosquitto broker;
      setUp(() async {
        broker = await Mosquitto.start();
      });
      tearDown(() => broker.dispose());

      test('empty client id with cleanStart false never connects', () async {
        var accepted = false;
        final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
        addTearDown(server.close);
        server.listen((socket) {
          accepted = true;
          socket.destroy();
        });
        final client = _client(server.port, '');
        await expectLater(
          client.connect(cleanStart: false),
          throwsArgumentError,
        );
        await settle(150);
        expect(accepted, isFalse);
      });

      test('in-session QoS 1 does not time out; a blocked publish does',
          () async {
        await broker.dispose();
        broker = await Mosquitto.start(config: ['max_inflight_messages 1']);
        final proxy = await FaultProxy.start(broker.port);
        addTearDown(proxy.close);
        final client = newClient(
          proxy.port,
          clientId: 'fixes-slot',
          autoReconnect: false,
          operationTimeout: const Duration(milliseconds: 400),
        );
        await client.connect();
        proxy.dropWhen =
            (frame) => frame.dir == Dir.s2c && frame.type == kPuback;
        final first = client
            .publish('fixes/slot', bytes('1'), qos: MqttQos.atLeastOnce)
            .then<Object>((result) => result, onError: (Object e) => e);
        await settle(600);
        expect(client.inflightCount, 1);
        await expectLater(
          client.publish('fixes/slot', bytes('2'), qos: MqttQos.atLeastOnce),
          throwsA(isA<MqttTimeoutException>()),
        );
        expect(proxy.sent(kPublish), hasLength(1));
        await client.close();
        expect(await first, isA<MqttConnectionException>());
      });

      test('a throwing listener stays up and the next message arrives',
          () async {
        final client = newClient(
          broker.port,
          clientId: 'fixes-listen',
          autoReconnect: false,
        );
        final errors = <Object>[];
        client.errors.listen(errors.add);
        await client.connect();
        await client.subscribe(
          'fixes/listen',
          options: const MqttSubscriptionOptions(qos: MqttQos.atLeastOnce),
        );
        final inbox = Inbox(client);
        client.messages.listen((message) {
          if (text(message.payload).startsWith('boom')) {
            throw StateError('listener bug');
          }
        });
        final publisher = newClient(
          broker.port,
          clientId: 'fixes-listen-pub',
          autoReconnect: false,
        );
        await publisher.connect();
        await publisher.publish(
          'fixes/listen',
          bytes('boom'),
          qos: MqttQos.atLeastOnce,
        );
        expect(errors, isNotEmpty);
        expect(client.state, MqttConnectionState.connected);
        await publisher.publish(
          'fixes/listen',
          bytes('next'),
          qos: MqttQos.atLeastOnce,
        );
        await inbox.waitFor(2);
        expect(inbox.payloads, ['boom', 'next']);
        await publisher.close();
        await client.close();
      });

      test('a fresh client does not resume a broker session', () async {
        final owner = newClient(
          broker.port,
          clientId: 'fixes-own',
          autoReconnect: false,
        );
        await owner.connect(sessionExpiryInterval: _sei);
        await owner.subscribe('fixes/own');
        await owner.disconnect();

        final fresh = newClient(
          broker.port,
          clientId: 'fixes-own',
          autoReconnect: false,
        );
        await expectLater(
          fresh.connect(cleanStart: false, sessionExpiryInterval: _sei),
          throwsA(isA<MqttProtocolException>()),
        );
        expect(fresh.state, MqttConnectionState.disconnected);

        await owner.connect(cleanStart: false, sessionExpiryInterval: _sei);
        expect(owner.sessionPresent, isTrue);
        await owner.close();
        await fresh.close();
      });

      test('Topic Alias 0 is an ArgumentError and the connection stays up',
          () async {
        final client = newClient(
          broker.port,
          clientId: 'fixes-alias0',
          autoReconnect: false,
        );
        await client.connect(topicAliasMaximum: 4);
        await expectLater(
          client.publish(
            'fixes/alias0',
            bytes('x'),
            properties: const [TopicAlias(0)],
          ),
          throwsArgumentError,
        );
        expect(client.state, MqttConnectionState.connected);
        await client.close();
      });

      test('empty subscribe and unsubscribe require a connection', () async {
        final client = _client(broker.port, 'fixes-empty');
        await expectLater(
          client.subscribeAll(const []),
          throwsA(isA<MqttConnectionException>()),
        );
        await expectLater(
          client.unsubscribe(const []),
          throwsA(isA<MqttConnectionException>()),
        );
        await client.connect();
        await client.subscribeAll(const []);
        await client.unsubscribe(const []);
        await client.close();
      });

      test('cleanStart false with expiry 0 warns and does not resume',
          () async {
        final log = CollectingLogger();
        final client = newClient(
          broker.port,
          clientId: 'fixes-expiry',
          autoReconnect: false,
          logger: log,
        );
        await client.connect(cleanStart: false);
        expect(
          log.lines
              .any((line) => line.contains('session expiry interval is 0')),
          isTrue,
        );
        await client.subscribe('fixes/expiry');
        await client.disconnect();
        await client.connect(cleanStart: false);
        expect(client.sessionPresent, isFalse);
        await client.close();
      });

      test('0x10 no matching subscribers is a success', () async {
        final client = newClient(
          broker.port,
          clientId: 'fixes-0x10',
          autoReconnect: false,
        );
        await client.connect();
        final result = await client.publish(
          'fixes/nobody',
          bytes('z'),
          qos: MqttQos.atLeastOnce,
        );
        expect(result.reasonCode, MqttReasonCode.noMatchingSubscribers);
        expect(result.isSuccess, isTrue);
        expect(result.isError, isFalse);
        await client.close();
      });

      test('changing connect settings while connected is a StateError',
          () async {
        final client = newClient(
          broker.port,
          clientId: 'fixes-join',
          autoReconnect: false,
        );
        await client.connect(keepAlive: const Duration(seconds: 30));
        await expectLater(
          client.connect(keepAlive: const Duration(seconds: 45)),
          throwsA(isA<StateError>()),
        );
        await expectLater(
          client.connect(keepAlive: const Duration(days: 100)),
          throwsArgumentError,
        );
        await client.connect(keepAlive: const Duration(seconds: 30));
        expect(client.state, MqttConnectionState.connected);
        final stopping = client.disconnect();
        expect(client.state, MqttConnectionState.disconnecting);
        await expectLater(
          client.connect(keepAlive: const Duration(seconds: 30)),
          throwsA(isA<StateError>()),
        );
        await stopping;
        await client.close();
      });

      test('the first PINGREQ is measured from CONNECT, not CONNACK', () async {
        final proxy = await FaultProxy.start(broker.port);
        addTearDown(proxy.close);
        proxy.s2c.paused = true;
        final client = newClient(
          proxy.port,
          clientId: 'fixes-ping',
          autoReconnect: false,
          pingResponseTimeout: const Duration(seconds: 30),
        );
        // Keep alive is a whole number of seconds on the wire. Hold CONNACK
        // most of that second: a clock started at CONNECT pings soon after
        // the hold, a clock started at CONNACK waits another full second.
        final connecting =
            client.connect(keepAlive: const Duration(seconds: 1));
        await proxy.next((frame) => frame.type == kConnect);
        await settle(700);
        final released = DateTime.now();
        proxy.s2c.paused = false;
        await connecting;
        final ping = await proxy.next((frame) => frame.type == kPingreq);
        expect(
          ping.at.difference(released),
          lessThan(const Duration(milliseconds: 500)),
        );
        await client.close();
      });
    });
  }

  await _emqxCases();
}

Future<void> _emqxCases() async {
  if (!await Emqx.available) {
    test('EMQX image not available', () {}, skip: 'docker image missing');
    return;
  }

  group('EMQX', () {
    late Emqx emqx;
    setUpAll(() async {
      emqx = await Emqx.start();
    });
    tearDownAll(() => emqx.dispose());

    test('a fresh client does not resume a broker session', () async {
      final owner = newClient(
        emqx.port,
        clientId: 'fixes-emqx-own',
        autoReconnect: false,
      );
      await owner.connect(sessionExpiryInterval: _sei);
      await owner.subscribe('fixes/emqx');
      await owner.disconnect();

      final fresh = newClient(
        emqx.port,
        clientId: 'fixes-emqx-own',
        autoReconnect: false,
      );
      await expectLater(
        fresh.connect(cleanStart: false, sessionExpiryInterval: _sei),
        throwsA(isA<MqttProtocolException>()),
      );
      await owner.connect(cleanStart: false, sessionExpiryInterval: _sei);
      expect(owner.sessionPresent, isTrue);
      await owner.close();
      await fresh.close();
    });

    test('0x10 no matching subscribers is a success', () async {
      final client = newClient(
        emqx.port,
        clientId: 'fixes-emqx-0x10',
        autoReconnect: false,
      );
      await client.connect();
      final result = await client.publish(
        'fixes/emqx-nobody',
        bytes('z'),
        qos: MqttQos.atLeastOnce,
      );
      expect(result.isError, isFalse);
      expect(result.isSuccess, isTrue);
      expect(result.reasonCode.value, lessThan(0x80));
      await client.close();
    });

    test('changing connect settings while connected is a StateError', () async {
      final client = newClient(
        emqx.port,
        clientId: 'fixes-emqx-join',
        autoReconnect: false,
      );
      await client.connect(keepAlive: const Duration(seconds: 30));
      await expectLater(
        client.connect(keepAlive: const Duration(seconds: 45)),
        throwsA(isA<StateError>()),
      );
      await client.connect(keepAlive: const Duration(seconds: 30));
      expect(client.state, MqttConnectionState.connected);
      await client.close();
    });
  });
}

MqttClient _client(int port, String clientId) => MqttClient(
      host: '127.0.0.1',
      port: port,
      clientId: clientId,
      autoReconnect: false,
      connectionTimeout: const Duration(seconds: 3),
    );

Future<void> _until(bool Function() ready) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!ready()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('condition not met');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

final class _RawPeer {
  _RawPeer._(this._server);

  final ServerSocket _server;
  final MqttPacketDecoder _decoder = MqttPacketDecoder();
  final List<MqttPacket> packets = [];
  final Completer<void> _connected = Completer<void>();
  Socket? _socket;

  int get port => _server.port;
  Future<void> get connected => _connected.future;

  static Future<_RawPeer> start() async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final peer = _RawPeer._(server);
    server.listen(peer._accept);
    return peer;
  }

  void _accept(Socket socket) {
    _socket = socket;
    socket.listen((Uint8List data) {
      _decoder.add(data);
      while (true) {
        final packet = _decoder.nextPacket();
        if (packet == null) {
          break;
        }
        packets.add(packet);
        if (packet is MqttConnectPacket && !_connected.isCompleted) {
          _connected.complete();
        }
      }
    }, onError: (_) {});
  }

  void send(List<MqttPacket> outgoing) {
    final builder = BytesBuilder();
    for (final packet in outgoing) {
      builder.add(MqttPacketCodec.encode(packet));
    }
    _socket!.add(builder.takeBytes());
  }

  Future<T> waitFor<T extends MqttPacket>() async {
    final deadline = DateTime.now().add(const Duration(seconds: 5));
    while (DateTime.now().isBefore(deadline)) {
      for (final packet in packets) {
        if (packet is T) {
          return packet;
        }
      }
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
    fail('no $T arrived; saw $packets');
  }

  Future<void> close() async {
    await _socket?.close();
    await _server.close();
  }
}
