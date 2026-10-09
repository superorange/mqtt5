@Tags(['real-broker'])
@Timeout(Duration(seconds: 60))
library;

import 'dart:convert';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

import 'support/common.dart';

void main() {
  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
    return;
  }

  late Mosquitto broker;
  late FaultProxy proxy;
  late MosqSub witness;
  setUp(() async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
    witness = await MosqSub.start(broker.port, ['will/#'], qos: 2);
  });
  tearDown(() async {
    await witness.stop();
    await proxy.close();
    await broker.dispose();
  });

  MqttWill will({
    MqttQos qos = MqttQos.atLeastOnce,
    bool retain = false,
    List<MqttProperty> properties = const [],
  }) =>
      MqttWill(
        topic: 'will/c',
        payload: Uint8List.fromList(utf8.encode('gone')),
        qos: qos,
        retain: retain,
        properties: properties,
      );

  test('abnormal loss publishes the Will with every Will property', () async {
    final c = newClient(proxy.port,
        clientId: 'w1',
        autoReconnect: false,
        will: will(qos: MqttQos.exactlyOnce, properties: [
          const PayloadFormatIndicator(1),
          const MessageExpiryInterval(90),
          const ContentType('text/plain'),
          const ResponseTopic('will/reply'),
          CorrelationData(utf8.encode('cid')),
          const UserProperty('why', 'crash'),
        ]));
    await c.connect();
    await broker.waitForLog('Will message specified (4 bytes) (r0, q2).');
    proxy.cutAll();
    await witness.waitFor(1);
    final m = witness.messages.single;
    final p = m['properties'] as Map<String, dynamic>;
    expect(m['topic'], 'will/c');
    expect(m['payload'], 'gone');
    expect(m['qos'], 2);
    expect(p['payload-format-indicator'], 1);
    expect(p['message-expiry-interval'], inInclusiveRange(88, 90));
    expect(p['content-type'], 'text/plain');
    expect(p['response-topic'], 'will/reply');
    expect(p['correlation-data'], 'cid');
    expect(p['user-properties'], [
      {'why': 'crash'}
    ]);
    await c.close();
  });

  test('normal DISCONNECT (0x00) discards the Will', () async {
    final c = newClient(proxy.port, clientId: 'w2', will: will());
    await c.connect();
    await c.disconnect();
    await settle(700);
    expect(witness.messages, isEmpty);
    await c.close();
  });

  test('DISCONNECT 0x04 publishes the Will', () async {
    final c = newClient(proxy.port, clientId: 'w3', will: will());
    await c.connect();
    await c.disconnect(reasonCode: MqttReasonCode.disconnectWithWillMessage);
    await witness.waitFor(1);
    expect(witness.messages.single['payload'], 'gone');
    await c.close();
  });

  test('Will Delay Interval postpones the Will', () async {
    final c = newClient(proxy.port,
        clientId: 'w4',
        autoReconnect: false,
        will: will(properties: const [WillDelayInterval(2)]));
    await c.connect(sessionExpiryInterval: const Duration(seconds: 30));
    // On the wire: Will Properties carry Will Delay Interval = 2.
    final connect = proxy.sent(kConnect).single.bytes;
    expect(_containsSeq(connect, [0x18, 0, 0, 0, 2]), isTrue);
    final sw = Stopwatch()..start();
    proxy.cutAll();
    await settle(500);
    expect(witness.messages, isEmpty, reason: 'must not be immediate');
    await witness.waitFor(1, timeout: const Duration(seconds: 6));
    // mosquitto evaluates will delays on a one-second tick.
    expect(sw.elapsedMilliseconds, greaterThanOrEqualTo(900));
    await c.close();
  });

  test('reconnecting within the Will Delay Interval cancels the Will',
      () async {
    final c = newClient(proxy.port,
        clientId: 'w5', will: will(properties: const [WillDelayInterval(3)]));
    await c.connect(sessionExpiryInterval: const Duration(seconds: 30));
    proxy.cutAll();
    await waitUntil(() =>
        proxy.connections >= 2 && c.state == MqttConnectionState.connected);
    await settle(4000);
    expect(witness.messages, isEmpty);
    await c.disconnect();
    await c.close();
  });

  test('retained Will reaches a late subscriber', () async {
    final c = newClient(proxy.port,
        clientId: 'w6', autoReconnect: false, will: will(retain: true));
    await c.connect();
    proxy.cutAll();
    await witness.waitFor(1);
    final late = newClient(broker.port, clientId: 'w6-late');
    await late.connect();
    final inbox = Inbox(late);
    await late.subscribe('will/c');
    await inbox.waitFor(1);
    expect(inbox.messages.single.retain, isTrue);
    await c.close();
    await late.close();
  });

  test(
      'a dead link (no PINGREQ reaches the broker) makes the broker publish '
      'the Will after 1.5 x keep alive', () async {
    final c = newClient(proxy.port,
        clientId: 'w7',
        autoReconnect: false,
        will: will(),
        pingResponseTimeout: const Duration(seconds: 30));
    await c.connect(keepAlive: const Duration(seconds: 1));
    proxy.blackhole = true;
    await witness.waitFor(1, timeout: const Duration(seconds: 5));
    await c.close();
  });

  test('an invalid Will topic is rejected before anything is sent', () {
    expect(
        () => newClient(proxy.port,
            will: MqttWill(topic: 'will/+', payload: Uint8List(0))),
        throwsArgumentError);
  });
}

bool _containsSeq(List<int> haystack, List<int> needle) {
  for (var i = 0; i + needle.length <= haystack.length; i++) {
    var ok = true;
    for (var j = 0; j < needle.length; j++) {
      if (haystack[i + j] != needle[j]) {
        ok = false;
        break;
      }
    }
    if (ok) return true;
  }
  return false;
}
