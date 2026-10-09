import 'dart:async';

import 'package:mqtt5/src/session/packet_identifier_pool.dart';
import 'package:test/test.dart';

/// Takes every identifier except [except].
void _fill(PacketIdentifierPool pool, {Set<int> except = const {}}) {
  for (var id = pool.tryAllocate(); id != null; id = pool.tryAllocate()) {}
  for (final id in except) {
    pool.release(id);
  }
}

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
      expect(pool.tryAllocate(), 1);
      expect(pool.tryAllocate(), 2);
      expect(pool.tryAllocate(), 3);
    });

    test('releases identifiers for reuse', () async {
      final pool = PacketIdentifierPool();
      final id = await pool.allocate();
      pool.release(id);
      // Take every other identifier so the released one is the only
      // remaining choice.
      _fill(pool, except: {id});
      final again = await pool.allocate();
      expect(again, id);
    });

    test('wraps around at 65535', () {
      final pool = PacketIdentifierPool();
      for (var i = 1; i <= 65535; i++) {
        expect(pool.tryAllocate(), i);
      }
      expect(pool.tryAllocate(), isNull);
      pool.release(7);
      expect(pool.tryAllocate(), 7);
    });

    test('allocate waits for a release when exhausted', () async {
      final pool = PacketIdentifierPool();
      _fill(pool);
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
      pool.tryAllocate();
      pool.tryAllocate();
      pool.reset();
      expect(pool.tryAllocate(), 1);
    });

    test('reset wakes an allocator waiting on exhaustion', () async {
      final pool = PacketIdentifierPool();
      _fill(pool);

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
