/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.loadbalance;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * An RSocket stored in {@link RSocketPool} that can resolve the target RSocket to use from an
 * {@link LoadbalanceRSocketSource}
 */
final class PooledWeightedRSocket extends ResolvingOperator<RSocket>
    implements CoreSubscriber<RSocket>, RSocket {

  final RSocketPool parent;
  final LoadbalanceRSocketSource loadbalanceRSocketSource;
  @Nullable final StatsTracker statsTracker;

  volatile Subscription s;

  static final AtomicReferenceFieldUpdater<PooledWeightedRSocket, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(PooledWeightedRSocket.class, Subscription.class, "s");

  PooledWeightedRSocket(
      RSocketPool parent,
      LoadbalanceRSocketSource loadbalanceRSocketSource,
      LoadbalanceStrategy strategy) {
    this.parent = parent;
    this.statsTracker = strategy instanceof StatsTracker ? (StatsTracker) strategy : null;
    this.loadbalanceRSocketSource = loadbalanceRSocketSource;
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (Operators.setOnce(S, this, s)) {
      s.request(Long.MAX_VALUE);
    }
  }

  @Override
  public void onComplete() {
    final Subscription s = this.s;
    final RSocket value = this.value;

    if (s == Operators.cancelledSubscription() || !S.compareAndSet(this, s, null)) {
      this.doFinally();
      return;
    }

    if (value == null) {
      this.terminate(new IllegalStateException("Source completed empty"));
    } else {
      this.complete(value);
    }
  }

  @Override
  public void onError(Throwable t) {
    final Subscription s = this.s;

    if (s == Operators.cancelledSubscription()
        || S.getAndSet(this, Operators.cancelledSubscription())
            == Operators.cancelledSubscription()) {
      this.doFinally();
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    this.doFinally();
    // terminate upstream which means retryBackoff has exhausted
    this.terminate(t);
  }

  @Override
  public void onNext(RSocket value) {
    if (this.s == Operators.cancelledSubscription()) {
      this.doOnValueExpired(value);
      return;
    }

    this.value = value;
    // volatile write and check on racing
    this.doFinally();
  }

  @Override
  protected void doSubscribe() {
    this.loadbalanceRSocketSource.source().subscribe(this);
  }

  @Override
  protected void doOnValueResolved(RSocket value) {
    statsTracker.onRSocketAvailable(this);
    value.onClose().subscribe(null, t -> this.invalidate(), this::invalidate);
  }

  @Override
  protected void doOnValueExpired(RSocket value) {
    value.dispose();
    this.dispose();
  }

  @Override
  public void dispose() {
    super.dispose();
  }

  @Override
  protected void doOnDispose() {
    final RSocketPool parent = this.parent;
    for (; ; ) {
      final PooledWeightedRSocket[] sockets = parent.activeSockets;
      final int activeSocketsCount = sockets.length;

      int index = -1;
      for (int i = 0; i < activeSocketsCount; i++) {
        if (sockets[i] == this) {
          index = i;
          break;
        }
      }

      if (index == -1) {
        break;
      }

      final int lastIndex = activeSocketsCount - 1;
      final PooledWeightedRSocket[] newSockets = new PooledWeightedRSocket[lastIndex];
      if (index != 0) {
        System.arraycopy(sockets, 0, newSockets, 0, index);
      }

      if (index != lastIndex) {
        System.arraycopy(sockets, index + 1, newSockets, index, lastIndex - index);
      }

      if (RSocketPool.ACTIVE_SOCKETS.compareAndSet(parent, sockets, newSockets)) {
        break;
      }
    }
    Operators.terminate(S, this);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return new RequestTrackingMonoInner<>(this, payload, FrameType.REQUEST_FNF);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return new RequestTrackingMonoInner<>(this, payload, FrameType.REQUEST_RESPONSE);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return new RequestTrackingFluxInner<>(this, payload, FrameType.REQUEST_STREAM);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new RequestTrackingFluxInner<>(this, payloads, FrameType.REQUEST_CHANNEL);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return new RequestTrackingMonoInner<>(this, payload, FrameType.METADATA_PUSH);
  }

  LoadbalanceRSocketSource source() {
    return loadbalanceRSocketSource;
  }

  @Override
  public double availability() {
    return statsTracker != null ? statsTracker.getAvailabilityFor(this) : isDisposed() ? 0.0 : 1.0;
  }

  static final class RequestTrackingMonoInner<RESULT>
      extends MonoDeferredResolution<RESULT, RSocket> {

    long startTime;

    RequestTrackingMonoInner(PooledWeightedRSocket parent, Payload payload, FrameType requestType) {
      super(parent, payload, requestType);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(RSocket rSocket, Throwable t) {
      if (isTerminated()) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.payload);
        onError(t);
        return;
      }

      if (rSocket != null) {
        Mono<?> source;
        switch (this.requestType) {
          case REQUEST_FNF:
            source = rSocket.fireAndForget(this.payload);
            break;
          case REQUEST_RESPONSE:
            source = rSocket.requestResponse(this.payload);
            break;
          case METADATA_PUSH:
            source = rSocket.metadataPush(this.payload);
            break;
          default:
            Operators.error(this.actual, new IllegalStateException("Should never happen"));
            return;
        }

        PooledWeightedRSocket rsocketParent = (PooledWeightedRSocket) parent;
        startTime = rsocketParent.statsTracker.startOfRequest(rsocketParent);

        source.subscribe((CoreSubscriber) this);
      } else {
        parent.add(this);
      }
    }

    @Override
    public void onComplete() {
      final long state = this.requested;
      if (state != TERMINATED_STATE && REQUESTED.compareAndSet(this, state, TERMINATED_STATE)) {
        PooledWeightedRSocket rsocketParent = (PooledWeightedRSocket) parent;
        rsocketParent.statsTracker.endOfRequest(startTime, null, rsocketParent);
        super.onComplete();
      }
    }

    @Override
    public void onError(Throwable t) {
      final long state = this.requested;
      if (state != TERMINATED_STATE && REQUESTED.compareAndSet(this, state, TERMINATED_STATE)) {
        PooledWeightedRSocket rsocketParent = (PooledWeightedRSocket) parent;
        rsocketParent.statsTracker.endOfRequest(startTime, t, rsocketParent);
        super.onError(t);
      }
    }

    @Override
    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
      if (state == STATE_TERMINATED) {
        return;
      }

      if (state == STATE_SUBSCRIBED) {
        this.s.cancel();
        PooledWeightedRSocket rsocketParent = (PooledWeightedRSocket) parent;
        rsocketParent.statsTracker.endOfRequest(startTime, null, rsocketParent);
      } else {
        this.parent.remove(this);
        ReferenceCountUtil.safeRelease(this.payload);
      }
    }
  }

  static final class RequestTrackingFluxInner<INPUT>
      extends FluxDeferredResolution<INPUT, RSocket> {

    RequestTrackingFluxInner(
        PooledWeightedRSocket parent, INPUT fluxOrPayload, FrameType requestType) {
      super(parent, fluxOrPayload, requestType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accept(RSocket rSocket, Throwable t) {
      if (isTerminated()) {
        return;
      }

      if (t != null) {
        if (this.requestType == FrameType.REQUEST_STREAM) {
          ReferenceCountUtil.safeRelease(this.fluxOrPayload);
        }
        onError(t);
        return;
      }

      if (rSocket != null) {
        Flux<? extends Payload> source;
        switch (this.requestType) {
          case REQUEST_STREAM:
            source = rSocket.requestStream((Payload) this.fluxOrPayload);
            break;
          case REQUEST_CHANNEL:
            source = rSocket.requestChannel((Flux<Payload>) this.fluxOrPayload);
            break;
          default:
            Operators.error(this.actual, new IllegalStateException("Should never happen"));
            return;
        }

        PooledWeightedRSocket rsocketParent = (PooledWeightedRSocket) parent;
        rsocketParent.statsTracker.startOfStream(rsocketParent);

        source.subscribe(this);
      } else {
        parent.add(this);
      }
    }

    @Override
    public void onComplete() {
      final long state = this.requested;
      if (state != TERMINATED_STATE && REQUESTED.compareAndSet(this, state, TERMINATED_STATE)) {
        PooledWeightedRSocket rsocketParent = (PooledWeightedRSocket) parent;
        rsocketParent.statsTracker.endOfStream(rsocketParent);
        super.onComplete();
      }
    }

    @Override
    public void onError(Throwable t) {
      final long state = this.requested;
      if (state != TERMINATED_STATE && REQUESTED.compareAndSet(this, state, TERMINATED_STATE)) {
        PooledWeightedRSocket rsocketParent = (PooledWeightedRSocket) parent;
        rsocketParent.statsTracker.endOfStream(rsocketParent);
        super.onError(t);
      }
    }

    @Override
    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
      if (state == STATE_TERMINATED) {
        return;
      }

      if (state == STATE_SUBSCRIBED) {
        this.s.cancel();
        PooledWeightedRSocket rsocketParent = (PooledWeightedRSocket) parent;
        rsocketParent.statsTracker.endOfStream(rsocketParent);
      } else {
        this.parent.remove(this);
        if (requestType == FrameType.REQUEST_STREAM) {
          ReferenceCountUtil.safeRelease(this.fluxOrPayload);
        }
      }
    }
  }
}
