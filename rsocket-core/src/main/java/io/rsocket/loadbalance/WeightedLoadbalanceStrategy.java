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

import io.rsocket.RSocket;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import reactor.util.annotation.Nullable;

public class WeightedLoadbalanceStrategy implements LoadbalanceStrategy, StatsTracker {

  private static final double EXP_FACTOR = 4.0;

  private static final int EFFORT = 5;

  final SplittableRandom splittableRandom;
  final int effort;
  final Supplier<Stats> statsSupplier;
  final Map<RSocket, Stats> statsMap = new ConcurrentHashMap<>();

  public WeightedLoadbalanceStrategy() {
    this(EFFORT);
  }

  public WeightedLoadbalanceStrategy(int effort) {
    this(effort, new SplittableRandom(System.nanoTime()));
  }

  public WeightedLoadbalanceStrategy(int effort, SplittableRandom splittableRandom) {
    this(effort, splittableRandom, Stats::create);
  }

  public WeightedLoadbalanceStrategy(
      int effort, SplittableRandom splittableRandom, Supplier<Stats> statsSupplier) {
    this.splittableRandom = splittableRandom;
    this.effort = effort;
    this.statsSupplier = statsSupplier;
  }

  @Override
  public RSocket select(List<RSocket> sockets) {
    final int effort = this.effort;
    final int size = sockets.size();

    RSocket weightedRSocket;
    switch (size) {
      case 1:
        weightedRSocket = sockets.get(0);
        break;
      case 2:
        {
          RSocket rsc1 = sockets.get(0);
          RSocket rsc2 = sockets.get(1);

          double w1 = algorithmicWeight(rsc1);
          double w2 = algorithmicWeight(rsc2);
          if (w1 < w2) {
            weightedRSocket = rsc2;
          } else {
            weightedRSocket = rsc1;
          }
        }
        break;
      default:
        {
          RSocket rsc1 = null;
          RSocket rsc2 = null;

          for (int i = 0; i < effort; i++) {
            int i1 = ThreadLocalRandom.current().nextInt(size);
            int i2 = ThreadLocalRandom.current().nextInt(size - 1);

            if (i2 >= i1) {
              i2++;
            }
            rsc1 = sockets.get(i1);
            rsc2 = sockets.get(i2);
            if (rsc1.availability() > 0.0 && rsc2.availability() > 0.0) {
              break;
            }
          }

          double w1 = algorithmicWeight(rsc1);
          double w2 = algorithmicWeight(rsc2);
          if (w1 < w2) {
            weightedRSocket = rsc2;
          } else {
            weightedRSocket = rsc1;
          }
        }
    }

    return weightedRSocket;
  }

  private double algorithmicWeight(@Nullable final RSocket rsocket) {
    if (rsocket == null || rsocket.isDisposed() || rsocket.availability() == 0.0) {
      statsMap.remove(rsocket);
      return 0.0;
    }
    final Stats stats = statsMap.get(rsocket);
    final int pending = stats.pending();
    double latency = stats.predictedLatency();

    final double low = stats.lowerQuantileLatency();
    final double high =
        Math.max(
            stats.higherQuantileLatency(),
            low * 1.001); // ensure higherQuantile > lowerQuantile + .1%
    final double bandWidth = Math.max(high - low, 1);

    if (latency < low) {
      latency /= calculateFactor(low, latency, bandWidth);
    } else if (latency > high) {
      latency *= calculateFactor(latency, high, bandWidth);
    }

    return rsocket.availability() * 1.0 / (1.0 + latency * (pending + 1));
  }

  private static double calculateFactor(final double u, final double l, final double bandWidth) {
    final double alpha = (u - l) / bandWidth;
    return Math.pow(1 + alpha, EXP_FACTOR);
  }

  @Override
  public void onRSocketAvailable(RSocket rsocket) {
    rsocket
        .onClose()
        .doOnTerminate(
            () -> {
              Stats stats = statsMap.remove(rsocket);
              if (stats != null) {
                stats.setAvailability(0.0);
              }
            });
    statsMap.put(rsocket, statsSupplier.get());
  }

  @Override
  public double getAvailabilityFor(RSocket rsocket) {
    Stats stats = statsMap.get(rsocket);
    return stats != null ? stats.availability() : 0.0;
  }

  @Override
  public long startOfRequest(RSocket rsocket) {
    Stats stats = statsMap.get(rsocket);
    return stats != null ? stats.startRequest() : -1;
  }

  @Override
  public void endOfRequest(long startTime, @Nullable Throwable t, RSocket rsocket) {
    Stats stats = statsMap.get(rsocket);
    if (stats != null) {
      if (t != null) {
        stats.stopRequest(startTime);
        stats.recordError(0.0);
      } else {
        final long now = stats.stopRequest(startTime);
        stats.record(now - startTime);
      }
    }
  }

  @Override
  public void startOfStream(RSocket rsocket) {
    Stats stats = statsMap.get(rsocket);
    if (stats != null) {
      stats.startStream();
    }
  }

  @Override
  public void endOfStream(RSocket rsocket) {
    Stats stats = statsMap.get(rsocket);
    if (stats != null) {
      stats.stopStream();
    }
  }
}
