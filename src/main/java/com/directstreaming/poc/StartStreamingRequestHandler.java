package com.directstreaming.poc;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import static com.directstreaming.poc.CqlProtocolUtil.constructStartupMessage;

/**
 * Dispatch incoming requests. Logic is simple (and for now quite resource wasting) :
 * <p>
 * 1. Receive channelActive event - channel is open.
 * 2. Create bootstrap for outgoing Cassandra connection.
 *  2.1 send STARTUP
 *  2.2 send query -> forward to connection from point 1.
 *  2.n send query for n and n+1 (streams)  -> forward to connection from point 1.
 *  2.n+1 close.
 * </p>
 */
@ChannelHandler.Sharable
public class StartStreamingRequestHandler extends ChannelInboundHandlerAdapter {

    private final EventLoopGroup group;

    public StartStreamingRequestHandler(final EventLoopGroup group) {
        this.group = group;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {

        /*
         * why in isActive method ?
         * because it uses internally method that "Return {@code true} if the {@link Channel} is active and so connected."
         */

        //lets start streaming
        final CassandraRequestHandler cassandraRequestHandler = new CassandraRequestHandler(ctx.channel());

        /*
         * new Bootstrap.
         * FIXME - lets resume it or something, this is large resource wasting.
         */
        final Bootstrap bootstrap = new Bootstrap();

        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(cassandraRequestHandler);
                    }
                });

        final ChannelFuture connectionFuture = bootstrap.connect("127.0.0.1", 9042);

        connectionFuture.addListener(result ->
        {
            if (result.isSuccess()) {

                final Channel channel = connectionFuture.channel();

                final ByteBufAllocator alloc = channel.alloc();

                final ByteBuf buffer = alloc.directBuffer();

                /*
                 * construct STARTUP message - say hello to Cassandra node.
                 *
                 * fixme this is shit :( use one connection over all incoming remote connections.
                 */
                constructStartupMessage(buffer);

                channel.writeAndFlush(buffer);

            } else {
                //fixme, what now ? at least close connection.
                ctx.channel().close();
            }
        });


    }
}
