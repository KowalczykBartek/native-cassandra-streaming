package com.directstreaming.poc;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;

import static com.directstreaming.poc.CassandraStartupResponseHandler.STARTUP_MESSAGE_HANDLER_NAME;
import static com.directstreaming.poc.CqlProtocolUtil.constructStartupMessage;

/**
 * Dump Cassandra connector - attempt to query cassandra without any external driver.
 * Main goal is to reduce garbage and reduce unnecessary copying from direct buffer to user space and next to
 * direct buffers used by sockets.
 * This code is actually worse than horrible - but works - at least for me (｡◕‿‿◕｡)
 */
public class Client {

    static Logger LOG = Logger.getLogger(Client.class);

    public static void main(final String... args) throws InterruptedException {

        final EventLoopGroup group = new NioEventLoopGroup(1);

        try {

            final QueryManager queryManager = new QueryManager();

            final Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(STARTUP_MESSAGE_HANDLER_NAME,new CassandraStartupResponseHandler(null, queryManager));
                        }
                    });

            final ChannelFuture sync = b.connect("127.0.0.1", 9042).sync();

            final Channel cassandraChannel = sync.channel();
            final ByteBufAllocator alloc = cassandraChannel.alloc();

            final ByteBuf buffer = alloc.directBuffer();

            constructStartupMessage(buffer); //construct STARTUP message - say hello to Cassandra node.

            cassandraChannel.writeAndFlush(buffer);

            cassandraChannel.closeFuture().await();

        } catch (final InterruptedException ex) {
            LOG.error("Houston we have a problem ", ex);
        } finally {
            group.shutdownGracefully();
        }

    }
}
