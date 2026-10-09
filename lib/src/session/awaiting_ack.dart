/// When an outgoing QoS 1/2 exchange last put a packet on the wire that the
/// broker still owes an answer to: the PUBLISH (awaiting PUBACK/PUBREC) or
/// the PUBREL (awaiting PUBCOMP).
///
/// Only an entry stamped with the current connection's epoch is waiting on
/// that connection. One still queued for retransmission after a reconnect
/// keeps the stamp of an older connection and is not timed.
mixin AwaitingAck {
  /// The connection epoch the packet was written on; -1 before the first
  /// write.
  int sentEpoch = -1;

  /// Monotonic time of that write, in microseconds.
  int sentAtMicros = 0;

  void markSent(int epoch, int atMicros) {
    sentEpoch = epoch;
    sentAtMicros = atMicros;
  }
}
