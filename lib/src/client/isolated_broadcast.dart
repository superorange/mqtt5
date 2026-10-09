import 'dart:async';
import 'dart:collection';

/// A broadcast stream that isolates each listener.
///
/// Dart's broadcast [StreamController] does not catch exceptions thrown by
/// listeners. On an async controller that becomes an uncaught zone error and
/// can kill the isolate; on a sync controller it also aborts every listener
/// registered after the one that threw. MQTT delivery must not do either:
/// one broken subscriber is reported and the others still receive the event.
///
/// Listener callbacks run synchronously inside [add], so the caller can
/// acknowledge a publication only after the delivery round. As with any
/// [Stream], each callback runs in the [Zone] that was current when
/// [Stream.listen] was called, so zone values and zone-specific timers seen by
/// a listener do not depend on where the event came from. [onListen] is the
/// exception to synchronous delivery: a subscriber such as [Stream.first]
/// installs its data handler after [Stream.listen] returns, so the owner
/// should defer the first flush to a microtask.
final class IsolatedBroadcast<T> {
  IsolatedBroadcast({this.onListen, this.onListenerError});

  /// Called when the first listener subscribes. The listener is already
  /// registered, so [hasListener] is true.
  final void Function()? onListen;

  /// Called when a listener throws. The error does not escape [add].
  final void Function(Object error, StackTrace stackTrace)? onListenerError;

  final List<_IsolatedSubscription<T>> _subscriptions = [];
  bool _closed = false;

  bool get isClosed => _closed;

  bool get hasListener => _subscriptions.isNotEmpty;

  Stream<T> get stream => _IsolatedStream<T>(this);

  void add(T event) {
    if (_closed) {
      throw StateError('Cannot add event after closing');
    }
    final subscriptions = List<_IsolatedSubscription<T>>.of(_subscriptions);
    for (final subscription in subscriptions) {
      subscription._add(event);
    }
  }

  Future<void> close() {
    if (_closed) {
      return Future<void>.value();
    }
    _closed = true;
    final subscriptions = List<_IsolatedSubscription<T>>.of(_subscriptions);
    _subscriptions.clear();
    for (final subscription in subscriptions) {
      subscription._close();
    }
    return Future<void>.value();
  }

  void _report(Object error, StackTrace stackTrace) {
    final report = onListenerError;
    if (report == null) {
      return;
    }
    try {
      report(error, stackTrace);
    } on Object {
      // The reporter is itself a listener boundary. Swallowing here is the
      // last line: re-entering [add] would recurse through the same callback.
    }
  }
}

final class _IsolatedStream<T> extends Stream<T> {
  _IsolatedStream(this._owner);

  final IsolatedBroadcast<T> _owner;

  @override
  bool get isBroadcast => true;

  @override
  StreamSubscription<T> listen(
    void Function(T event)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    final subscription = _IsolatedSubscription<T>(
      _owner,
      Zone.current,
      onData,
      onDone,
    );
    if (_owner._closed) {
      scheduleMicrotask(subscription._close);
      return subscription;
    }
    final first = _owner._subscriptions.isEmpty;
    _owner._subscriptions.add(subscription);
    if (first) {
      _owner.onListen?.call();
    }
    return subscription;
  }
}

final class _IsolatedSubscription<T> implements StreamSubscription<T> {
  _IsolatedSubscription(
    this._owner,
    this._zone,
    void Function(T event)? onData,
    void Function()? onDone,
  )   : _onData = onData == null
            ? null
            : _zone.registerUnaryCallback<void, T>(onData),
        _onDone = onDone == null ? null : _zone.registerCallback<void>(onDone);

  final IsolatedBroadcast<T> _owner;

  /// The zone [Stream.listen] was called in; every callback runs there.
  final Zone _zone;
  void Function(T event)? _onData;
  void Function()? _onDone;
  int _pauseCount = 0;
  bool _canceled = false;
  bool _closed = false;

  /// Set when the stream closes while events are still buffered or the
  /// subscription is paused. Those events are delivered before [onDone].
  bool _pendingDone = false;
  final ListQueue<T> _buffer = ListQueue<T>();

  bool get _paused => _pauseCount > 0;

  void _add(T event) {
    if (_canceled || _closed || _pendingDone) {
      return;
    }
    if (_paused) {
      _buffer.add(event);
      return;
    }
    _invokeData(event);
  }

  void _invokeData(T event) {
    final onData = _onData;
    if (onData == null) {
      return;
    }
    try {
      _zone.runUnary<void, T>(onData, event);
    } on Object catch (error, stackTrace) {
      _owner._report(error, stackTrace);
    }
  }

  void _close() {
    if (_canceled || _closed || _pendingDone) {
      return;
    }
    if (_paused || _buffer.isNotEmpty) {
      _pendingDone = true;
      return;
    }
    _finish();
  }

  void _finish() {
    if (_canceled || _closed) {
      return;
    }
    _closed = true;
    _pendingDone = false;
    _buffer.clear();
    _owner._subscriptions.remove(this);
    final onDone = _onDone;
    if (onDone == null) {
      return;
    }
    try {
      _zone.run<void>(onDone);
    } on Object catch (error, stackTrace) {
      _owner._report(error, stackTrace);
    }
  }

  void _drain() {
    while (_buffer.isNotEmpty && !_paused && !_canceled) {
      _invokeData(_buffer.removeFirst());
    }
    if (_pendingDone && !_paused && !_canceled) {
      _finish();
    }
  }

  @override
  Future<void> cancel() {
    if (_canceled) {
      return Future<void>.value();
    }
    _canceled = true;
    _buffer.clear();
    _owner._subscriptions.remove(this);
    return Future<void>.value();
  }

  @override
  void onData(void Function(T data)? handleData) {
    _onData = handleData == null
        ? null
        : _zone.registerUnaryCallback<void, T>(handleData);
  }

  @override
  void onError(Function? handleError) {
    // This stream has no error events. Listener failures are reported through
    // [IsolatedBroadcast.onListenerError] and do not complete the subscription.
    if (handleError == null) {
      return;
    }
  }

  @override
  void onDone(void Function()? handleDone) {
    _onDone =
        handleDone == null ? null : _zone.registerCallback<void>(handleDone);
  }

  @override
  void pause([Future<void>? resumeSignal]) {
    if (_canceled || _closed) {
      return;
    }
    _pauseCount++;
    if (resumeSignal != null) {
      resumeSignal.then<void>(
        (_) {
          resume();
        },
        onError: (Object error, StackTrace stackTrace) {
          // The caller's resume signal failed. Report it like a listener
          // failure instead of raising an uncaught error, and resume anyway.
          _owner._report(error, stackTrace);
          resume();
        },
      );
    }
  }

  @override
  void resume() {
    if (_pauseCount == 0) {
      return;
    }
    _pauseCount--;
    if (_pauseCount == 0) {
      _drain();
    }
  }

  @override
  bool get isPaused => _pauseCount > 0;

  @override
  Future<E> asFuture<E>([E? futureValue]) {
    final value = futureValue as E;
    final completer = Completer<E>();
    onData((_) {});
    onError((Object error, StackTrace stackTrace) {
      if (!completer.isCompleted) {
        completer.completeError(error, stackTrace);
      }
      cancel();
    });
    onDone(() {
      if (!completer.isCompleted) {
        completer.complete(value);
      }
    });
    return completer.future;
  }
}
