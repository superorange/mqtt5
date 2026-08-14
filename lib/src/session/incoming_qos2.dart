/// Tracks incoming QoS 2 messages to de-duplicate retransmissions.
///
/// An entry exists from the moment a QoS 2 PUBLISH is received (and its
/// PUBREC is sent) until the matching PUBREL is handled and PUBCOMP is sent.
final class IncomingQos2Store {
  final Set<int> _inProgress = <int>{};

  int get count => _inProgress.length;

  bool contains(int packetIdentifier) => _inProgress.contains(packetIdentifier);

  /// Registers a newly received QoS 2 PUBLISH. Returns false if it was
  /// already in progress (a duplicate).
  bool add(int packetIdentifier) => _inProgress.add(packetIdentifier);

  /// Completes the exchange after PUBREL/PUBCOMP.
  void remove(int packetIdentifier) => _inProgress.remove(packetIdentifier);

  void clear() => _inProgress.clear();
}
