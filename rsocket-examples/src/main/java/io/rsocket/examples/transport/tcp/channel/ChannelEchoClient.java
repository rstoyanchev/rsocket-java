/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.examples.transport.tcp.channel;

import io.rsocket.*;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.stream.Collectors;

public final class ChannelEchoClient {

  private static final Logger logger = LoggerFactory.getLogger(ChannelEchoClient.class);


  public static void main(String[] args) {
    RSocketFactory.receive()
        .fragment(64)
        .frameDecoder(PayloadDecoder.ZERO_COPY)
        .acceptor(new SocketAcceptorImpl())
        .transport(TcpServerTransport.create(7000))
        .start()
        .subscribe();

    RSocket socket =
        RSocketFactory.connect()
            .fragment(64)
            .keepAliveAckTimeout(Duration.ofMinutes(10))
            .frameDecoder(PayloadDecoder.ZERO_COPY)
            .transport(TcpClientTransport.create(7000))
            .start()
            .block();

      Flux<Payload> input = Flux.just("A", "B", "C", "D", "E", "F", "G", "H", "I", "J")
              .flatMap(s -> Mono.just(s).repeat(1000).collect(Collectors.joining("", "[", "]")))
              .doOnNext(logger::debug)
              .map(ByteBufPayload::create);

      socket.requestChannel(input)
              .map(Payload::getDataUtf8)
              .doFinally(signalType -> socket.dispose())
              .then()
              .block();
  }

  private static class SocketAcceptorImpl implements SocketAcceptor {
    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket reactiveSocket) {
      return Mono.just(
          new AbstractRSocket() {

            @Override
            public Mono<Void> fireAndForget(Payload payload) {
              //                  System.out.println(payload.getDataUtf8());
              payload.release();
              return Mono.empty();
            }

            @Override
            public Mono<Payload> requestResponse(Payload payload) {
              return Mono.just(payload);
            }

            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              return Flux.from(payloads).subscribeOn(Schedulers.single());
            }
          });
    }
  }
}
