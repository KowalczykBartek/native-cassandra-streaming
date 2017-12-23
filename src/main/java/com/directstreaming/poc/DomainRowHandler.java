package com.directstreaming.poc;

import com.directstreaming.poc.domain.DomainRow;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class DomainRowHandler extends SimpleChannelInboundHandler<DomainRow> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DomainRow msg) throws Exception {
        System.err.println(msg.getValue1());
        System.err.println(msg.getValue2());
        System.err.println(msg.getValue3());
        System.err.println(msg.getValue4());
        System.err.println(msg.getValue5());
        System.err.println(msg.getValue6());
    }
}
