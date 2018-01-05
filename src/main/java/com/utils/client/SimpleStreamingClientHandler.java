package com.utils.client;

import com.directstreaming.poc.Server;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

public class SimpleStreamingClientHandler extends ChannelInboundHandlerAdapter {

    static Logger LOG = Logger.getLogger(SimpleStreamingClientHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        int i = ((ByteBuf) msg).readableBytes();

        byte[] message = new byte[i];
        ((ByteBuf) msg).readBytes(message);

        LOG.debug(new String(message));

        ((ByteBuf) msg).release();
    }
}
