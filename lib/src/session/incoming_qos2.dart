/// Tracks incoming QoS 2 messages to de-duplicate retransmissions.
///
/// An entry exists from the moment a QoS 2 PUBLISH is received until the
/// matching PUBREL is handled and PUBCOMP is sent. Each entry remembers the
/// network connection ("epoch") its PUBLISH last arrived on, because the
/// Receive Maximum quota is per connection (section 4.9): an exchange left
/// over from an earlier connection, now only waiting for its PUBREL, no
/// longer counts against the quota of the current one.
final class IncomingQos2Store {
  final Map<int, int> _epochOf = <int, int>{};

  /// Open exchanges per epoch, so the quota check is O(1) per PUBLISH.
  final Map<int, int> _countIn = <int, int>{};

  bool contains(int packetIdentifier) => _epochOf.containsKey(packetIdentifier);

  /// Whether [packetIdentifier] is held by a PUBLISH received on [epoch].
  bool isCurrent(int packetIdentifier, int epoch) =>
      _epochOf[packetIdentifier] == epoch;

  /// The exchanges whose PUBLISH arrived on [epoch] and are not complete.
  int countIn(int epoch) => _countIn[epoch] ?? 0;

  /// Records a QoS 2 PUBLISH received on [epoch]. Returns false if the
  /// identifier was already in progress (a duplicate).
  bool add(int packetIdentifier, int epoch) {
    final previous = _epochOf[packetIdentifier];
    if (previous != null) {
      _uncount(previous);
    }
    _epochOf[packetIdentifier] = epoch;
    _countIn[epoch] = (_countIn[epoch] ?? 0) + 1;
    return previous == null;
  }

  /// Completes the exchange after PUBREL/PUBCOMP.
  void remove(int packetIdentifier) {
    final epoch = _epochOf.remove(packetIdentifier);
    if (epoch != null) {
      _uncount(epoch);
    }
  }

  void clear() {
    _epochOf.clear();
    _countIn.clear();
  }

  void _uncount(int epoch) {
    final left = _countIn[epoch]! - 1;
    if (left == 0) {
      _countIn.remove(epoch);
    } else {
      _countIn[epoch] = left;
    }
  }
}
