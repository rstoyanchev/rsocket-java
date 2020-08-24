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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

class RSocketPool extends ResolvingOperator<Void>
    implements CoreSubscriber<List<LoadbalanceRSocketSource>>, List<RSocket> {

  final DeferredResolutionRSocket deferredResolutionRSocket = new DeferredResolutionRSocket(this);
  final LoadbalanceStrategy loadbalanceStrategy;

  volatile PooledWeightedRSocket[] activeSockets;

  static final AtomicReferenceFieldUpdater<RSocketPool, PooledWeightedRSocket[]> ACTIVE_SOCKETS =
      AtomicReferenceFieldUpdater.newUpdater(
          RSocketPool.class, PooledWeightedRSocket[].class, "activeSockets");

  static final PooledWeightedRSocket[] EMPTY = new PooledWeightedRSocket[0];
  static final PooledWeightedRSocket[] TERMINATED = new PooledWeightedRSocket[0];

  volatile Subscription s;
  static final AtomicReferenceFieldUpdater<RSocketPool, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(RSocketPool.class, Subscription.class, "s");

  RSocketPool(
      Publisher<List<LoadbalanceRSocketSource>> source, LoadbalanceStrategy loadbalanceStrategy) {
    this.loadbalanceStrategy = loadbalanceStrategy;

    ACTIVE_SOCKETS.lazySet(this, EMPTY);

    source.subscribe(this);
  }

  @Override
  protected void doOnDispose() {
    Operators.terminate(S, this);

    RSocket[] activeSockets = ACTIVE_SOCKETS.getAndSet(this, TERMINATED);
    for (RSocket rSocket : activeSockets) {
      rSocket.dispose();
    }
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (Operators.setOnce(S, this, s)) {
      s.request(Long.MAX_VALUE);
    }
  }

  /**
   * This operation should happen rarely relatively compares the number of the {@link #select()}
   * method invocations, therefore it is acceptable to have it algorithmically inefficient. The
   * algorithmic complexity of this method is
   *
   * @param loadbalanceRSocketSources set which represents RSocket source to balance on
   */
  @Override
  public void onNext(List<LoadbalanceRSocketSource> loadbalanceRSocketSources) {
    if (isDisposed()) {
      return;
    }

    PooledWeightedRSocket[] previouslyActiveSockets;
    PooledWeightedRSocket[] activeSockets;
    for (; ; ) {
      HashMap<LoadbalanceRSocketSource, Integer> rSocketSuppliersCopy = new HashMap<>();

      int j = 0;
      for (LoadbalanceRSocketSource loadbalanceRSocketSource : loadbalanceRSocketSources) {
        rSocketSuppliersCopy.put(loadbalanceRSocketSource, j++);
      }

      // checking intersection of active RSocket with the newly received set
      previouslyActiveSockets = this.activeSockets;
      PooledWeightedRSocket[] nextActiveSockets =
          new PooledWeightedRSocket[previouslyActiveSockets.length + rSocketSuppliersCopy.size()];
      int position = 0;
      for (int i = 0; i < previouslyActiveSockets.length; i++) {
        PooledWeightedRSocket rSocket = previouslyActiveSockets[i];

        Integer index = rSocketSuppliersCopy.remove(rSocket.source());
        if (index == null) {
          // if one of the active rSockets is not included, we remove it and put in the
          // pending removal
          if (!rSocket.isDisposed()) {
            rSocket.dispose();
            // TODO: provide a meaningful algo for keeping removed rsocket in the list
            //            nextActiveSockets[position++] = rSocket;
          }
        } else {
          if (!rSocket.isDisposed()) {
            // keep old RSocket instance
            nextActiveSockets[position++] = rSocket;
          } else {
            // put newly create RSocket instance
            nextActiveSockets[position++] =
                new PooledWeightedRSocket(
                    this, loadbalanceRSocketSources.get(index), loadbalanceStrategy);
          }
        }
      }

      // going though brightly new rsocket
      for (LoadbalanceRSocketSource newLoadbalanceRSocketSource : rSocketSuppliersCopy.keySet()) {
        nextActiveSockets[position++] =
            new PooledWeightedRSocket(this, newLoadbalanceRSocketSource, loadbalanceStrategy);
      }

      // shrank to actual length
      if (position == 0) {
        activeSockets = EMPTY;
      } else {
        activeSockets = Arrays.copyOf(nextActiveSockets, position);
      }

      if (ACTIVE_SOCKETS.compareAndSet(this, previouslyActiveSockets, activeSockets)) {
        break;
      }
    }

    if (isPending()) {
      // notifies that upstream is resolved
      if (activeSockets != EMPTY) {
        //noinspection ConstantConditions
        complete(null);
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    // indicates upstream termination
    S.set(this, Operators.cancelledSubscription());
    // propagates error and terminates the whole pool
    terminate(t);
  }

  @Override
  public void onComplete() {
    // indicates upstream termination
    S.set(this, Operators.cancelledSubscription());
  }

  RSocket select() {
    if (isDisposed()) {
      return this.deferredResolutionRSocket;
    }

    RSocket selected = doSelect();

    if (selected == null) {
      if (this.s == Operators.cancelledSubscription()) {
        terminate(new CancellationException("Pool is exhausted"));
      } else {
        invalidate();
      }
      return this.deferredResolutionRSocket;
    }

    return selected;
  }

  @Nullable
  RSocket doSelect() {
    RSocket[] sockets = this.activeSockets;
    if (sockets == EMPTY) {
      return null;
    }

    return this.loadbalanceStrategy.select(this);
  }

  @Override
  public RSocket get(int index) {
    return activeSockets[index];
  }

  @Override
  public int size() {
    return activeSockets.length;
  }

  @Override
  public boolean isEmpty() {
    return activeSockets.length == 0;
  }

  @Override
  public Object[] toArray() {
    return activeSockets;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(T[] a) {
    return (T[]) activeSockets;
  }

  static class DeferredResolutionRSocket implements RSocket {

    final RSocketPool parent;

    DeferredResolutionRSocket(RSocketPool parent) {
      this.parent = parent;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      return new MonoInner<>(this.parent, payload, FrameType.REQUEST_FNF);
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return new MonoInner<>(this.parent, payload, FrameType.REQUEST_RESPONSE);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return new FluxInner<>(this.parent, payload, FrameType.REQUEST_STREAM);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return new FluxInner<>(this.parent, payloads, FrameType.REQUEST_STREAM);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
      return new MonoInner<>(this.parent, payload, FrameType.METADATA_PUSH);
    }
  }

  static final class MonoInner<T> extends MonoDeferredResolution<T, Void> {

    MonoInner(RSocketPool parent, Payload payload, FrameType requestType) {
      super(parent, payload, requestType);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(Void aVoid, Throwable t) {
      if (isTerminated()) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.payload);
        onError(t);
        return;
      }

      RSocketPool parent = (RSocketPool) this.parent;
      RSocket rSocket = parent.doSelect();
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

        source.subscribe((CoreSubscriber) this);
      } else {
        parent.add(this);
      }
    }
  }

  static final class FluxInner<INPUT> extends FluxDeferredResolution<INPUT, Void> {

    FluxInner(RSocketPool parent, INPUT fluxOrPayload, FrameType requestType) {
      super(parent, fluxOrPayload, requestType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accept(Void aVoid, Throwable t) {
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

      RSocketPool parent = (RSocketPool) this.parent;
      RSocket rSocket = parent.doSelect();
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

        source.subscribe(this);
      } else {
        parent.add(this);
      }
    }
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<RSocket> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(RSocket rsocket) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends RSocket> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(int index, Collection<? extends RSocket> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RSocket set(int index, RSocket element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(int index, RSocket element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RSocket remove(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int indexOf(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int lastIndexOf(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListIterator<RSocket> listIterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListIterator<RSocket> listIterator(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RSocket> subList(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException();
  }
}
