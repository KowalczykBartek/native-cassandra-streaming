package com.directstreaming.poc;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;

import static com.directstreaming.poc.CassandraStartupResponseHandler.STARTUP_MESSAGE_HANDLER_NAME;
import static com.directstreaming.poc.CqlProtocolUtil.constructStartupMessage;

/**
 * Dispatch incoming requests. Logic is simple (and for now quite resource wasting) :
 * <p>
 * 1. Receive channelActive event - channel is open.
 * 2. Create bootstrap for outgoing Cassandra connection.
 * 2.1 send STARTUP
 * 2.2 send query -> forward to connection from point 1.
 * 2.n send query for n and n+1 (streams)  -> forward to connection from point 1.
 * 2.n+1 close.
 * </p>
 */
public class StartStreamingRequestHandler extends ChannelInboundHandlerAdapter {

    private static Logger LOG = Logger.getLogger(StartStreamingRequestHandler.class);

    private final EventLoopGroup group;

    private Runnable lastCallback;

    public StartStreamingRequestHandler(final EventLoopGroup group) {
        this.group = group;
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {

        if (ctx.channel().isWritable()) {
            if (lastCallback != null) {
                lastCallback.run();

                lastCallback = null;
            }
        }
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {

        /*
         * why in isActive method ?
         * because it uses internally method that "Return {@code true} if the {@link Channel} is active and so connected."
         */


        //lets start streaming
        final QueryManager queryManager = new QueryManager();

        final StreamingBridge bridge = new StreamingBridge(ctx.alloc().directBuffer(1024));

        bridge.pokeMe = () -> {

            if (ctx.executor().inEventLoop()) {

                if (ctx.channel().isWritable()) {
                    bridge.interThreadBuffer.retain();
                    ChannelFuture writeFuture = ctx.channel().write(bridge.interThreadBuffer);
                    ctx.flush();
                    writeFuture.addListener(result -> {
                        bridge.interThreadBuffer.discardReadBytes();
                        bridge.continueReading.run();
                    });

                } else {
                    lastCallback = () -> {
                        bridge.interThreadBuffer.retain();
                        ChannelFuture writeFuture = ctx.channel().write(bridge.interThreadBuffer);
                        ctx.flush();
                        writeFuture.addListener(result -> {
                            bridge.interThreadBuffer.discardReadBytes();

                            bridge.continueReading.run();
                        });
                    };
                }

            } else {

                ctx.executor().execute(() -> {

                    if (ctx.channel().isWritable()) {

                        bridge.interThreadBuffer.retain();
                        ChannelFuture writeFuture = ctx.channel().write(bridge.interThreadBuffer);
                        ctx.flush();
                        writeFuture.addListener(result -> {
                            bridge.interThreadBuffer.discardReadBytes();
                            bridge.continueReading.run();
                        });

                    } else {
                        ctx.channel().flush();

                        lastCallback = () -> {

                            bridge.interThreadBuffer.retain();
                            ChannelFuture writeFuture = ctx.channel().write(bridge.interThreadBuffer);
                            ctx.flush();
                            writeFuture.addListener(result -> {
                                bridge.interThreadBuffer.discardReadBytes();
                                bridge.continueReading.run();
                            });
                        };
                    }
                });
            }
        };

        bridge.cleanupIncoming = () -> {
            bridge.interThreadBuffer.release();
            LOG.info("Close incoming connection - thanks for streaming !");
            ctx.channel().close();
        };

        final CassandraStartupResponseHandler cassandraStartupResponseHandler = new CassandraStartupResponseHandler(bridge, queryManager);

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
                        p.addLast(STARTUP_MESSAGE_HANDLER_NAME, cassandraStartupResponseHandler);
                    }
                });

        final ChannelFuture connectionFuture = bootstrap.connect("127.0.0.1", 9042);

        connectionFuture.addListener(result ->
        {
            if (result.isSuccess()) {

                final Channel channel = connectionFuture.channel();

                final ByteBufAllocator alloc = channel.alloc();

                final ByteBuf buffer = alloc.heapBuffer(35);

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
