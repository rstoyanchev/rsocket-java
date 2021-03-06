package io.rsocket.fragmentation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameUtil;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.util.DefaultPayload;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class FragmentationIntegrationTest {
  private static byte[] data = new byte[128];
  private static byte[] metadata = new byte[128];

  static {
    ThreadLocalRandom.current().nextBytes(data);
    ThreadLocalRandom.current().nextBytes(metadata);
  }

  private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @DisplayName("fragments and reassembles data")
  @Test
  void fragmentAndReassembleData() {
    ByteBuf frame =
        PayloadFrameCodec.encodeNextCompleteReleasingPayload(
            allocator, 2, DefaultPayload.create(data));
    System.out.println(FrameUtil.toString(frame));

    frame.retain();

    Publisher<ByteBuf> fragments =
        FrameFragmenter.fragmentFrame(allocator, 64, frame, FrameHeaderCodec.frameType(frame));

    FrameReassembler reassembler = new FrameReassembler(allocator, Integer.MAX_VALUE);

    ByteBuf assembled =
        Flux.from(fragments)
            .doOnNext(byteBuf -> System.out.println(FrameUtil.toString(byteBuf)))
            .handle(reassembler::reassembleFrame)
            .blockLast();

    System.out.println("assembled");
    String s = FrameUtil.toString(assembled);
    System.out.println(s);

    Assert.assertEquals(FrameHeaderCodec.frameType(frame), FrameHeaderCodec.frameType(assembled));
    Assert.assertEquals(frame.readableBytes(), assembled.readableBytes());
    Assert.assertEquals(PayloadFrameCodec.data(frame), PayloadFrameCodec.data(assembled));
  }
}
