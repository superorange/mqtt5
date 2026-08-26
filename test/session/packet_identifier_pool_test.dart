import 'dart:async';

import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:mqtt5/src/session/packet_identifier_pool.dart';
import 'package:test/test.dart';

void main() {
  group('PacketIdentifierPool', () {
    test('allocates sequential identifiers', () {
      final pool = PacketIdentifierPool();
      expect(pool.allocate(), completion(1));
      expect(pool.allocate(), completion(2));
      expect(pool.allocate(), completion(3));
    });

    test('does not reuse in-flight identifiers', () {
      final pool = PacketIdentifierPool();
      pool.reserve(1);
      expect(pool.tryAllocate(), 2);
      expect(pool.tryAllocate(), 3);
    });

    test('releases identifiers for reuse', () async {
      final pool = PacketIdentifierPool();
      final id = await pool.allocate();
      pool.release(id);
      // Reserve every other identifier so the released one is the only
      // remaining choice.
      for (var i = 1; i <= 65535; i++) {
        if (i != id) {
          pool.reserve(i);
        }
      }
      final again = await pool.allocate();
      expect(again, id);
    });

    test('reserve rejects duplicates and out of range', () {
      final pool = PacketIdentifierPool();
      pool.reserve(10);
      expect(() => pool.reserve(10), throwsA(isA<MqttFlowControlException>()));
      expect(() => pool.reserve(0), throwsA(isA<MqttFlowControlException>()));
      expect(
          () => pool.reserve(65536), throwsA(isA<MqttFlowControlException>()));
    });

    test('wraps around at 65535', () {
      final pool = PacketIdentifierPool();
      pool.reserve(65535);
      for (var i = 0; i < 65534; i++) {
        expect(pool.tryAllocate(), isNotNull);
      }
      expect(pool.tryAllocate(), isNull);
      expect(pool.inUseCount, 65535);
    });

    test('allocate waits for a release when exhausted', () async {
      final pool = PacketIdentifierPool();
      // Reserve everything except 1..2 are already allocated below.
      for (var i = 1; i <= 65535; i++) {
        pool.reserve(i);
      }
      final future = pool.allocate();
      var completed = false;
      unawaited(future.then((_) => completed = true));
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(completed, isFalse);
      pool.release(5);
      expect(await future.timeout(const Duration(seconds: 1)), 5);
    });

    test('reset clears all identifiers', () {
      final pool = PacketIdentifierPool();
      pool.reserve(3);
      pool.reset();
      expect(pool.tryAllocate(), 1);
    });

    test('reset wakes an allocator waiting on exhaustion', () async {
      final pool = PacketIdentifierPool();
      for (var i = 1; i <= PacketIdentifierPool.maxIdentifier; i++) {
        pool.reserve(i);
      }

      final waiting = pool.allocate();
      await Future<void>.delayed(Duration.zero);
      pool.reset();

      expect(
        await waiting.timeout(const Duration(seconds: 1)),
        1,
      );
    });
  });
}
