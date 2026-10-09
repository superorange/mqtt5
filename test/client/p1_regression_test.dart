import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:mqtt5/testing.dart';
import 'package:test/test.dart';

void main() {
  test(
      'session resume retransmits every inflight QoS1 when Receive Maximum is 1',
      () async {
    final transports = <MemoryTransport>[];
    final client = MqttClient(
      host: 'h',
      transportFactory: () {
        final transport = MemoryTransport();
        transports.add(transport);
        return transport;
      },
      operationTimeout: Duration.zero,
      reconnectManager: ReconnectManager(
        initialDelay: const Duration(milliseconds: 5),
        maxDelay: const Duration(milliseconds: 5),
        jitterFactor: 0,
      ),
    );

    final connecting = client.connect(cleanStart: false);
    await _waitFor(() => transports.isNotEmpty);
    await _nextPacket(transports.last);
    transports.last.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          properties: [ReceiveMaximum(10)],
        ),
      ),
    );
    await connecting;

    final pending = <Future<MqttPublishResult>>[];
    for (final topic in ['a', 'b', 'c', 'd']) {
      pending.add(
        client.publish(topic, Uint8List(1), qos: MqttQos.atLeastOnce),
      );
    }
    await _waitFor(() => client.inflightCount == 4);
    transports.last.takeOutgoing();
    expect(client.inflightCount, 4);

    transports.last.injectError(MqttTransportException('drop'));
    await _waitFor(() => transports.length == 2);
    final t2 = transports.last;
    await _nextPacket(t2);
    t2.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: true,
          properties: [ReceiveMaximum(1)],
        ),
      ),
    );
    await _waitFor(() => t2.outgoing.isNotEmpty);
    final snapshot = t2.takeOutgoingBytes();
    expect(snapshot, isNotEmpty);
    final queued = MqttPacketDecoder().feed(snapshot);
    expect(queued, isNotEmpty, reason: 'decoded ${snapshot.length} bytes');

    final retransmitted = <String>[];
    Future<MqttPublishPacket> nextPublish() async {
      if (queued.isNotEmpty) {
        final packet = queued.removeAt(0);
        expect(packet, isA<MqttPublishPacket>(), reason: 'got $packet');
        return packet as MqttPublishPacket;
      }
      final packet = await _nextPacket(t2);
      expect(packet, isA<MqttPublishPacket>(), reason: 'got $packet');
      return packet as MqttPublishPacket;
    }

    for (var i = 0; i < 4; i++) {
      final publish = await nextPublish();
      retransmitted.add(publish.topicName);
      t2.inject(
        MqttPacketCodec.encode(
          MqttPubackPacket(packetIdentifier: publish.packetIdentifier),
        ),
      );
      if (i < 3) {
        await _waitFor(
          () => t2.outgoing.isNotEmpty,
        );
      }
    }

    expect(retransmitted, ['a', 'b', 'c', 'd']);
    for (final future in pending) {
      expect((await future).reasonCode, MqttReasonCode.success);
    }
    await client.close();
  });

  test('CONNACK and PUBACK in one chunk do not deadlock resume', () async {
    final transports = <MemoryTransport>[];
    final client = MqttClient(
      host: 'h',
      transportFactory: () {
        final transport = MemoryTransport();
        transports.add(transport);
        return transport;
      },
      operationTimeout: Duration.zero,
      reconnectManager: ReconnectManager(
        initialDelay: const Duration(milliseconds: 5),
        maxDelay: const Duration(milliseconds: 5),
        jitterFactor: 0,
      ),
    );

    final connecting = client.connect(cleanStart: false);
    await _waitFor(() => transports.isNotEmpty);
    await _nextPacket(transports.last);
    transports.last.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          properties: [ReceiveMaximum(10)],
        ),
      ),
    );
    await connecting;

    final pendingA =
        client.publish('a', Uint8List(1), qos: MqttQos.atLeastOnce);
    final pubA = await _nextPacket(transports.last) as MqttPublishPacket;
    final pendingB =
        client.publish('b', Uint8List(1), qos: MqttQos.atLeastOnce);
    final pubB = await _nextPacket(transports.last) as MqttPublishPacket;
    expect(client.inflightCount, 2);

    transports.last.injectError(MqttTransportException('drop'));
    await _waitFor(() => transports.length == 2);
    final t2 = transports.last;
    await _nextPacket(t2);

    final connack = MqttPacketCodec.encode(
      const MqttConnackPacket(
        sessionPresent: true,
        properties: [ReceiveMaximum(1)],
      ),
    );
    final ack = MqttPacketCodec.encode(
      MqttPubackPacket(packetIdentifier: pubA.packetIdentifier),
    );
    t2.inject(Uint8List.fromList([...connack, ...ack]));

    expect(
      (await pendingA.timeout(const Duration(milliseconds: 500))).reasonCode,
      MqttReasonCode.success,
    );

    final retransmit = await _nextPacket(t2)
        .timeout(const Duration(milliseconds: 500)) as MqttPublishPacket;
    expect(retransmit.packetIdentifier, pubB.packetIdentifier);
    expect(retransmit.dup, isTrue);
    expect(client.state, MqttConnectionState.connected);

    t2.inject(
      MqttPacketCodec.encode(
        MqttPubackPacket(packetIdentifier: pubB.packetIdentifier),
      ),
    );
    expect((await pendingB).reasonCode, MqttReasonCode.success);
    await client.close();
  });

  test('CONNACK and PUBLISH in one chunk use the new session, not the old one',
      () async {
    final transports = <MemoryTransport>[];
    final client = MqttClient(
      host: 'h',
      transportFactory: () {
        final transport = MemoryTransport();
        transports.add(transport);
        return transport;
      },
      reconnectManager: ReconnectManager(
        initialDelay: const Duration(milliseconds: 5),
        maxDelay: const Duration(milliseconds: 5),
        jitterFactor: 0,
      ),
    );

    final messages = <String>[];
    client.messages.listen((message) => messages.add(message.topic));

    final connecting = client.connect();
    await _waitFor(() => transports.isNotEmpty);
    await _nextPacket(transports.last);
    transports.last.inject(
      MqttPacketCodec.encode(const MqttConnackPacket(sessionPresent: false)),
    );
    await connecting;

    transports.last.inject(
      MqttPacketCodec.encode(
        MqttPublishPacket(
          topicName: 'first',
          payload: Uint8List(1),
          qos: MqttQos.exactlyOnce,
          packetIdentifier: 100,
        ),
      ),
    );
    await _nextPacket(transports.last); // PUBREC

    transports.last.injectError(MqttTransportException('drop'));
    await _waitFor(() => transports.length == 2);
    await _nextPacket(transports.last);

    final second = MqttPacketCodec.encode(
      const MqttConnackPacket(sessionPresent: false),
    );
    final publish = MqttPacketCodec.encode(
      MqttPublishPacket(
        topicName: 'second',
        payload: Uint8List(1),
        qos: MqttQos.exactlyOnce,
        packetIdentifier: 100,
      ),
    );
    transports.last.inject(
      Uint8List.fromList([...second, ...publish]),
    );

    await _waitFor(() => messages.contains('second'));
    expect(messages, ['first', 'second']);
    await client.close();
  });

  test('malformed packet during handshake fails connect()', () async {
    final transport = MemoryTransport();
    final client = MqttClient(
      host: 'h',
      autoReconnect: false,
      transportFactory: () => transport,
    );

    final connecting = client.connect();
    await _nextPacket(transport);
    transport.inject(Uint8List.fromList([0x00, 0x00]));

    await expectLater(connecting, throwsA(isA<MqttException>()));
    expect(client.state, MqttConnectionState.disconnected);
    await client.close();
  });

  test('timed-out QoS1 publish is retransmitted after session resume',
      () async {
    final transports = <MemoryTransport>[];
    final client = MqttClient(
      host: 'h',
      transportFactory: () {
        final transport = MemoryTransport();
        transports.add(transport);
        return transport;
      },
      operationTimeout: const Duration(milliseconds: 40),
      reconnectManager: ReconnectManager(
        initialDelay: const Duration(milliseconds: 5),
        maxDelay: const Duration(milliseconds: 5),
        jitterFactor: 0,
      ),
    );

    final connecting = client.connect(cleanStart: false);
    await _waitFor(() => transports.isNotEmpty);
    await _nextPacket(transports.last);
    transports.last.inject(
      MqttPacketCodec.encode(const MqttConnackPacket(sessionPresent: false)),
    );
    await connecting;

    Object? publishError;
    final published =
        client.publish('t', Uint8List(1), qos: MqttQos.atLeastOnce);
    published.then((_) {}, onError: (Object error) {
      publishError = error;
    });
    await Future<void>.delayed(const Duration(milliseconds: 80));
    expect(publishError, isNull);
    expect(client.inflightCount, 1);
    transports.last.takeOutgoing();

    transports.last.injectError(MqttTransportException('drop'));
    await _waitFor(() => transports.length == 2);
    await _nextPacket(transports.last);
    transports.last.inject(
      MqttPacketCodec.encode(const MqttConnackPacket(sessionPresent: true)),
    );

    final retransmit = await _nextPacket(transports.last) as MqttPublishPacket;
    expect(retransmit.topicName, 't');
    expect(retransmit.dup, isTrue);
    await client.close();
  });

  test('Error from transportFactory on reconnect is fatal', () async {
    var attempts = 0;
    final first = MemoryTransport();
    final client = MqttClient(
      host: 'h',
      transportFactory: () {
        attempts++;
        if (attempts == 1) {
          return first;
        }
        throw StateError('factory failed');
      },
      reconnectManager: ReconnectManager(
        initialDelay: const Duration(milliseconds: 5),
        maxDelay: const Duration(milliseconds: 5),
        jitterFactor: 0,
      ),
    );

    final connecting = client.connect();
    await _nextPacket(first);
    first.inject(
      MqttPacketCodec.encode(const MqttConnackPacket(sessionPresent: false)),
    );
    await connecting;

    final reported = client.errors.first;
    first.injectError(MqttTransportException('drop'));
    expect(
        await reported.timeout(const Duration(seconds: 1)), isA<StateError>());
    expect(client.state, MqttConnectionState.disconnected);
    await client.close();
  });
}

Future<void> _waitFor(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('Timed out waiting for condition');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

final _pendingPackets = <MemoryTransport, List<MqttPacket>>{};

Future<MqttPacket> _nextPacket(MemoryTransport transport) async {
  final queued = _pendingPackets.putIfAbsent(transport, () => <MqttPacket>[]);
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  final decoder = MqttPacketDecoder();
  while (true) {
    if (queued.isNotEmpty) {
      return queued.removeAt(0);
    }
    final bytes = transport.takeOutgoingBytes();
    if (bytes.isNotEmpty) {
      queued.addAll(decoder.feed(bytes));
      if (queued.isNotEmpty) {
        return queued.removeAt(0);
      }
    }
    if (DateTime.now().isAfter(deadline)) {
      fail(
        'Timed out waiting for a packet '
        '(queued=${queued.length} outgoing=${transport.outgoing.length})',
      );
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
