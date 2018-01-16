package com.directstreaming.poc;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class CassandraStartupResponseHandler extends ChannelInboundHandlerAdapter {

    public static final String STARTUP_MESSAGE_HANDLER_NAME = "startupMessageHandler";

    private final StreamingBridge bridge;

    private final QueryManager queryManager;

    public CassandraStartupResponseHandler(final StreamingBridge bridge, final QueryManager queryManager) {
        this.bridge = bridge;
        this.queryManager = queryManager;
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        //assumption is that STARTUP message is always ok and valid. of course FIXME

        //remove our-self (this is first and last startup message)
        ctx.pipeline().remove(STARTUP_MESSAGE_HANDLER_NAME);

        //install new handler
        CassandraPartitionQueryUtil.installNewHandlerAndPerformQuery(null, ctx.channel(), bridge, queryManager);
    }
}
