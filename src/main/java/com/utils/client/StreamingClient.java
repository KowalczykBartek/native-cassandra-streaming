package com.utils.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Our small hero - make request, print everything and close connection.
 */
public class StreamingClient {
    public static void main(final String[] args) throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)//
                    .channel(NioSocketChannel.class) //
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) throws Exception {
                            final ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new SimpleStreamingClientHandler());
                        }
                    });

            b.connect("127.0.0.1", 8080).sync().channel().closeFuture().sync();

        } finally {
            group.shutdownGracefully();
        }
    }
}
