package com.directstreaming.poc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * Dump Cassandra connector (exposed as Server util) - attempt to query cassandra without any external driver.
 * Main goal is to reduce garbage and reduce unnecessary copying from direct buffer to user space and next to
 * direct buffers used by sockets.
 * This code is actually worse than horrible - but works - at least for me (｡◕‿‿◕｡)
 *
 * Server listening for connection : 8080
 * Cassandra uses : 9042
 */
public class Server {

    public static void main(final String... args) throws InterruptedException {
        final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        final EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {

            final StartStreamingRequestHandler startStreamingRequestHandler = new StartStreamingRequestHandler(workerGroup);

            ServerBootstrap b = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 100)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(startStreamingRequestHandler);
                        }
                    });

           final  ChannelFuture f = b.bind(8080).sync();

            f.channel().closeFuture().sync();

        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }
}
