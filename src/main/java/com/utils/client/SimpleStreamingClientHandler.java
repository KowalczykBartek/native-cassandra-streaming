package com.utils.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

public class SimpleStreamingClientHandler extends ChannelInboundHandlerAdapter {

    static Logger LOG = Logger.getLogger(SimpleStreamingClientHandler.class);

    private long start;

    private final int thread;

    public SimpleStreamingClientHandler(final int thread) {
        this.thread = thread;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        start = System.currentTimeMillis();
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        final long time = (System.currentTimeMillis() - start) / 1000;
        LOG.info("[" + thread + "] Took " + time);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {

        int i = ((ByteBuf) msg).readableBytes();

        byte[] message = new byte[i];
        ((ByteBuf) msg).readBytes(message);

//        LOG.debug("[" + thread + "] " + new String(message));
        LOG.debug("[" + thread + "] got message");

        ((ByteBuf) msg).release();
    }
}
