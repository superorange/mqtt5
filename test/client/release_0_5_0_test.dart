// Behaviour introduced for 0.5.0: session ownership, the acknowledgement
// timeout, retry rules for automatic reconnects, re-subscription after a
// lost session, and listener zones.
import 'dart:async';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:mqtt5/testing.dart';
import 'package:test/test.dart';

void main() {
  group('session ownership (MQTT-3.2.2-4)', () {
    test('a new instance resuming a broker session fails and sends 0x82',
        () async {
      final rig = _Rig();
      final connecting = rig.client.connect(
        cleanStart: false,
        sessionExpiryInterval: const Duration(minutes: 1),
      );
      final t1 = await rig.nextTransport();
      final connect = await rig.next(t1) as MqttConnectPacket;
      expect(connect.cleanStart, isFalse);
      rig.connack(t1, sessionPresent: true);
      await expectLater(
          connecting, throwsA(isA<MqttSessionNotOwnedException>()));
      final disconnect = await rig.next(t1) as MqttDisconnectPacket;
      expect(disconnect.reasonCode, MqttReasonCode.protocolError);
      expect(rig.client.state, MqttConnectionState.disconnected);
      expect(rig.transports, hasLength(1), reason: 'no retry');
      await rig.client.close();
    });

    test('adoptBrokerSession takes the session over', () async {
      final rig = _Rig();
      final connecting = rig.client.connect(
        cleanStart: false,
        sessionExpiryInterval: const Duration(minutes: 1),
        adoptBrokerSession: true,
      );
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1, sessionPresent: true);
      await connecting;
      expect(rig.client.sessionPresent, isTrue);
      expect(rig.client.state, MqttConnectionState.connected);

      // A PUBREL for an exchange the previous holder began is answered.
      t1.inject(
          MqttPacketCodec.encode(const MqttPubrelPacket(packetIdentifier: 7)));
      final pubcomp = await rig.next(t1) as MqttPubcompPacket;
      expect(pubcomp.packetIdentifier, 7);

      // Reconnects of this instance resume as usual.
      t1.injectError(MqttTransportException('drop'));
      final t2 = await rig.nextTransport(2);
      expect((await rig.next(t2) as MqttConnectPacket).cleanStart, isFalse);
      rig.connack(t2, sessionPresent: true);
      await rig
          .waitFor(() => rig.client.state == MqttConnectionState.connected);
      await rig.client.close();
    });

    test('an instance that connected before resumes without adopting',
        () async {
      final rig = _Rig();
      final connecting =
          rig.client.connect(sessionExpiryInterval: const Duration(minutes: 1));
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;
      t1.injectError(MqttTransportException('drop'));
      final t2 = await rig.nextTransport(2);
      await rig.next(t2);
      rig.connack(t2, sessionPresent: true);
      await rig
          .waitFor(() => rig.client.state == MqttConnectionState.connected);
      await rig.client.close();
    });

    test('changing adoptBrokerSession while connected is a StateError',
        () async {
      final rig = _Rig();
      final connecting = rig.client.connect();
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;
      expect(
          () => rig.client.connect(adoptBrokerSession: true), throwsStateError);
      await rig.client.close();
    });
  });

  group('ackTimeout', () {
    test('a QoS 1 PUBACK that never comes replaces the connection', () async {
      final rig = _Rig(ackTimeout: const Duration(milliseconds: 200));
      final connecting =
          rig.client.connect(sessionExpiryInterval: const Duration(minutes: 1));
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;

      var settled = false;
      final publishing = rig.client
          .publish('t', Uint8List.fromList([1]), qos: MqttQos.atLeastOnce)
        ..whenComplete(() => settled = true);
      final first = await rig.next(t1) as MqttPublishPacket;
      expect(first.dup, isFalse);

      // No PUBACK: after ackTimeout the client says goodbye with 0x00 (the
      // session survives and the Will is not published) and reconnects.
      final disconnect = await rig.next(t1) as MqttDisconnectPacket;
      expect(disconnect.reasonCode ?? MqttReasonCode.success,
          MqttReasonCode.success);
      expect(settled, isFalse, reason: 'the publish future is not failed');

      final t2 = await rig.nextTransport(2);
      expect((await rig.next(t2) as MqttConnectPacket).cleanStart, isFalse);
      rig.connack(t2, sessionPresent: true);
      final resent = await rig.next(t2) as MqttPublishPacket;
      expect(resent.dup, isTrue);
      expect(resent.packetIdentifier, first.packetIdentifier);
      t2.inject(MqttPacketCodec.encode(
          MqttPubackPacket(packetIdentifier: resent.packetIdentifier)));
      expect((await publishing).reasonCode, MqttReasonCode.success);
      await rig.client.close();
    });

    test('a QoS 2 PUBCOMP that never comes replaces the connection', () async {
      final rig = _Rig(ackTimeout: const Duration(milliseconds: 200));
      final connecting =
          rig.client.connect(sessionExpiryInterval: const Duration(minutes: 1));
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;

      final publishing = rig.client
          .publish('t', Uint8List.fromList([2]), qos: MqttQos.exactlyOnce);
      final publish = await rig.next(t1) as MqttPublishPacket;
      final id = publish.packetIdentifier;
      t1.inject(MqttPacketCodec.encode(MqttPubrecPacket(packetIdentifier: id)));
      expect((await rig.next(t1) as MqttPubrelPacket).packetIdentifier, id);
      expect(await rig.next(t1), isA<MqttDisconnectPacket>());

      final t2 = await rig.nextTransport(2);
      await rig.next(t2);
      rig.connack(t2, sessionPresent: true);
      expect((await rig.next(t2) as MqttPubrelPacket).packetIdentifier, id);
      t2.inject(
          MqttPacketCodec.encode(MqttPubcompPacket(packetIdentifier: id)));
      expect((await publishing).reasonCode, MqttReasonCode.success);
      await rig.client.close();
    });

    test('acknowledged exchanges never trigger it', () async {
      final rig = _Rig(ackTimeout: const Duration(milliseconds: 150));
      final connecting = rig.client.connect();
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;
      for (var i = 0; i < 5; i++) {
        final publishing = rig.client
            .publish('t', Uint8List.fromList([i]), qos: MqttQos.atLeastOnce);
        final publish = await rig.next(t1) as MqttPublishPacket;
        t1.inject(MqttPacketCodec.encode(
            MqttPubackPacket(packetIdentifier: publish.packetIdentifier)));
        await publishing;
        await Future<void>.delayed(const Duration(milliseconds: 60));
      }
      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(rig.transports, hasLength(1));
      expect(rig.client.state, MqttConnectionState.connected);
      await rig.client.close();
    });

    test('Duration.zero disables it', () async {
      final rig = _Rig(ackTimeout: Duration.zero);
      final connecting = rig.client.connect();
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;
      unawaited(rig.client
          .publish('t', Uint8List(1), qos: MqttQos.atLeastOnce)
          .then((_) {}, onError: (_) {}));
      await rig.next(t1);
      await Future<void>.delayed(const Duration(milliseconds: 400));
      expect(rig.transports, hasLength(1));
      expect(rig.client.inflightCount, 1);
      await rig.client.close();
    });

    test('without autoReconnect the client stops and reports the timeout',
        () async {
      final rig = _Rig(
        ackTimeout: const Duration(milliseconds: 150),
        autoReconnect: false,
      );
      final errors = <Object>[];
      rig.client.errors.listen(errors.add);
      final connecting = rig.client.connect();
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;
      final publishing =
          rig.client.publish('t', Uint8List(1), qos: MqttQos.atLeastOnce);
      await rig.next(t1);
      await expectLater(publishing, throwsA(isA<MqttTimeoutException>()));
      await rig.waitFor(() => errors.isNotEmpty);
      expect(errors.single, isA<MqttTimeoutException>());
      expect(rig.client.state, MqttConnectionState.disconnected);
      await rig.client.close();
    });

    test('rejects a negative value', () {
      expect(
        () => MqttClient(host: 'h', ackTimeout: const Duration(seconds: -1)),
        throwsArgumentError,
      );
    });
  });

  group('automatic reconnect retry rules', () {
    test('a protocol error in the CONNACK read retries instead of stopping',
        () async {
      final rig = _Rig();
      final errors = <Object>[];
      rig.client.errors.listen(errors.add);
      final connecting =
          rig.client.connect(sessionExpiryInterval: const Duration(minutes: 1));
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;

      t1.injectError(MqttTransportException('drop'));
      final t2 = await rig.nextTransport(2);
      await rig.next(t2);
      // CONNACK plus a PINGREQ, which a server must never send, in one read.
      t2.inject(Uint8List.fromList([
        ...MqttPacketCodec.encode(
            const MqttConnackPacket(sessionPresent: true)),
        ...MqttPacketCodec.encode(const MqttPingreqPacket()),
      ]));
      final disconnect = await rig.next(t2) as MqttDisconnectPacket;
      expect(disconnect.reasonCode, MqttReasonCode.protocolError);

      final t3 = await rig.nextTransport(3);
      await rig.next(t3);
      rig.connack(t3, sessionPresent: true);
      await rig
          .waitFor(() => rig.client.state == MqttConnectionState.connected);
      expect(errors, isEmpty);
      await rig.client.close();
    });

    test('the same error on the initial connect is thrown to the caller',
        () async {
      final rig = _Rig();
      final connecting = rig.client.connect();
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      t1.inject(Uint8List.fromList([
        ...MqttPacketCodec.encode(
            const MqttConnackPacket(sessionPresent: false)),
        ...MqttPacketCodec.encode(const MqttPingreqPacket()),
      ]));
      await expectLater(connecting, throwsA(isA<MqttProtocolException>()));
      expect(rig.transports, hasLength(1));
      await rig.client.close();
    });

    test('CONNACK 0x85 on a reconnect is retried', () async {
      final rig = _Rig();
      final connecting = rig.client.connect();
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;
      t1.injectError(MqttTransportException('drop'));
      final t2 = await rig.nextTransport(2);
      await rig.next(t2);
      rig.connack(t2, reasonCode: MqttReasonCode.clientIdentifierNotValid);
      final t3 = await rig.nextTransport(3);
      await rig.next(t3);
      rig.connack(t3);
      await rig
          .waitFor(() => rig.client.state == MqttConnectionState.connected);
      await rig.client.close();
    });

    test('CONNACK 0x85 on the initial connect is thrown', () async {
      final rig = _Rig();
      final connecting = rig.client.connect();
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1, reasonCode: MqttReasonCode.clientIdentifierNotValid);
      await expectLater(
        connecting,
        throwsA(isA<MqttServerRejectedException>()
            .having((e) => e.reasonCode, 'reasonCode', 0x85)),
      );
      await rig.client.close();
    });

    test('bad credentials on a reconnect still stop the client', () async {
      final rig = _Rig();
      final errors = <Object>[];
      rig.client.errors.listen(errors.add);
      final connecting = rig.client.connect();
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;
      t1.injectError(MqttTransportException('drop'));
      final t2 = await rig.nextTransport(2);
      await rig.next(t2);
      rig.connack(t2, reasonCode: MqttReasonCode.badUserNameOrPassword);
      await rig.waitFor(() => errors.isNotEmpty);
      expect(errors.single, isA<MqttServerRejectedException>());
      expect(rig.client.state, MqttConnectionState.disconnected);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(rig.transports, hasLength(2));
      await rig.client.close();
    });
  });

  group('connect() while an automatic reconnect runs', () {
    test('waits for the reconnect instead of returning early', () async {
      final rig = _Rig();
      final connecting = rig.client.connect();
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;
      t1.injectError(MqttTransportException('drop'));
      await rig
          .waitFor(() => rig.client.state == MqttConnectionState.reconnecting);
      var joined = false;
      final joining = rig.client.connect().then((_) => joined = true);
      final t2 = await rig.nextTransport(2);
      await rig.next(t2);
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(joined, isFalse);
      rig.connack(t2);
      await joining;
      expect(rig.client.state, MqttConnectionState.connected);
      await rig.client.close();
    });

    test('throws when the reconnect ends for good', () async {
      final rig = _Rig();
      rig.client.errors.listen((_) {});
      final connecting = rig.client.connect();
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;
      t1.injectError(MqttTransportException('drop'));
      await rig
          .waitFor(() => rig.client.state == MqttConnectionState.reconnecting);
      final joining = rig.client.connect();
      final t2 = await rig.nextTransport(2);
      await rig.next(t2);
      rig.connack(t2, reasonCode: MqttReasonCode.badUserNameOrPassword);
      await expectLater(
        joining,
        throwsA(isA<MqttServerRejectedException>()
            .having((e) => e.reasonCode, 'reasonCode', 0x86)),
      );
      await rig.client.close();
    });
  });

  test('a re-subscription cut short is sent again on a present session',
      () async {
    final rig = _Rig();
    final connecting =
        rig.client.connect(sessionExpiryInterval: const Duration(minutes: 1));
    final t1 = await rig.nextTransport();
    await rig.next(t1);
    rig.connack(t1);
    await connecting;
    final subscribing = rig.client.subscribe('a/b');
    final subscribe = await rig.next(t1) as MqttSubscribePacket;
    t1.inject(MqttPacketCodec.encode(MqttSubackPacket(
        packetIdentifier: subscribe.packetIdentifier, reasonCodes: [0])));
    await subscribing;

    // The broker lost the session: the client re-subscribes, but the
    // connection drops before the SUBACK.
    t1.injectError(MqttTransportException('drop'));
    final t2 = await rig.nextTransport(2);
    await rig.next(t2);
    rig.connack(t2);
    final resubscribe = await rig.next(t2) as MqttSubscribePacket;
    expect(resubscribe.subscriptions.single.topicFilter, 'a/b');
    t2.injectError(MqttTransportException('drop'));

    // The broker kept the new, empty session from t2 and reports it present.
    final t3 = await rig.nextTransport(3);
    await rig.next(t3);
    rig.connack(t3, sessionPresent: true);
    final again = await rig.next(t3) as MqttSubscribePacket;
    expect(again.subscriptions.single.topicFilter, 'a/b');
    t3.inject(MqttPacketCodec.encode(MqttSubackPacket(
        packetIdentifier: again.packetIdentifier, reasonCodes: [0])));
    await Future<void>.delayed(const Duration(milliseconds: 50));

    // Answered now: the next present session needs nothing.
    t3.injectError(MqttTransportException('drop'));
    final t4 = await rig.nextTransport(4);
    await rig.next(t4);
    rig.connack(t4, sessionPresent: true);
    await rig.waitFor(() => rig.client.state == MqttConnectionState.connected);
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(rig.drain(t4).whereType<MqttSubscribePacket>(), isEmpty);
    await rig.client.close();
  });

  group('listeners', () {
    test('run in the zone they subscribed in', () async {
      final rig = _Rig();
      final seen = <Object?>[];
      runZoned(
        () => rig.client.messages.listen((_) => seen.add(Zone.current[#tag])),
        zoneValues: {#tag: 'listener-zone'},
      );
      final connecting = rig.client.connect();
      final t1 = await rig.nextTransport();
      await rig.next(t1);
      rig.connack(t1);
      await connecting;
      t1.inject(MqttPacketCodec.encode(
          MqttPublishPacket(topicName: 't', payload: Uint8List(1))));
      await rig.waitFor(() => seen.isNotEmpty);
      expect(seen, ['listener-zone']);
      await rig.client.close();
    });

    test('a failing resume signal is reported, not left uncaught', () async {
      final rig = _Rig();
      final reported = <Object>[];
      final uncaught = <Object>[];
      await runZonedGuarded(() async {
        rig.client.errors.listen(reported.add);
        final sub = rig.client.messages.listen((_) {});
        sub.pause(Future<void>.error(StateError('resume signal failed')));
        await Future<void>.delayed(const Duration(milliseconds: 20));
        expect(sub.isPaused, isFalse);
      }, (error, _) => uncaught.add(error));
      expect(uncaught, isEmpty);
      expect(reported.single, isA<StateError>());
      await rig.client.close();
    });
  });
}

final class _Rig {
  _Rig({
    Duration ackTimeout = const Duration(seconds: 60),
    bool autoReconnect = true,
  }) {
    client = MqttClient(
      host: 'h',
      clientId: 'rig',
      ackTimeout: ackTimeout,
      autoReconnect: autoReconnect,
      transportFactory: () {
        final transport = MemoryTransport();
        transports.add(transport);
        return transport;
      },
      reconnectManager: ReconnectManager(
        initialDelay: const Duration(milliseconds: 5),
        maxDelay: const Duration(milliseconds: 5),
        jitterFactor: 0,
        flapWindow: Duration.zero,
        stableAfter: Duration.zero,
      ),
    );
  }

  late final MqttClient client;
  final List<MemoryTransport> transports = [];
  final Map<MemoryTransport, List<MqttPacket>> _queued = {};
  final Map<MemoryTransport, MqttPacketDecoder> _decoders = {};

  Future<MemoryTransport> nextTransport([int count = 1]) async {
    await waitFor(() => transports.length >= count);
    return transports[count - 1];
  }

  void connack(
    MemoryTransport transport, {
    bool sessionPresent = false,
    MqttReasonCode reasonCode = MqttReasonCode.success,
  }) {
    transport.inject(MqttPacketCodec.encode(MqttConnackPacket(
      sessionPresent: sessionPresent,
      reasonCode: reasonCode,
    )));
  }

  /// Every packet the client has written to [transport] so far.
  List<MqttPacket> drain(MemoryTransport transport) {
    final queued = _queued.putIfAbsent(transport, () => []);
    final decoder = _decoders.putIfAbsent(transport, MqttPacketDecoder.new);
    queued.addAll(decoder.feed(transport.takeOutgoingBytes()));
    final all = List<MqttPacket>.of(queued);
    queued.clear();
    return all;
  }

  Future<MqttPacket> next(MemoryTransport transport) async {
    final queued = _queued.putIfAbsent(transport, () => []);
    final decoder = _decoders.putIfAbsent(transport, MqttPacketDecoder.new);
    await waitFor(() {
      queued.addAll(decoder.feed(transport.takeOutgoingBytes()));
      return queued.isNotEmpty;
    });
    return queued.removeAt(0);
  }

  Future<void> waitFor(bool Function() condition) async {
    final deadline = DateTime.now().add(const Duration(seconds: 5));
    while (!condition()) {
      if (DateTime.now().isAfter(deadline)) {
        fail('Timed out waiting for condition');
      }
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
  }
}
