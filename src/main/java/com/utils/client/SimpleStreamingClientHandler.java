package com.utils.client;

import com.directstreaming.poc.Server;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

public class SimpleStreamingClientHandler extends ChannelInboundHandlerAdapter {

    static Logger LOG = Logger.getLogger(SimpleStreamingClientHandler.class);

    private long start;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        start = System.currentTimeMillis();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        final long time = (System.currentTimeMillis() - start) / 1000;
        LOG.info("Took " + time);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        int i = ((ByteBuf) msg).readableBytes();

        byte[] message = new byte[i];
        ((ByteBuf) msg).readBytes(message);

        LOG.debug(new String(message));

        ((ByteBuf) msg).release();
    }
}
