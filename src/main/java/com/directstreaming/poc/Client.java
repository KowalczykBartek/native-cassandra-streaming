package com.directstreaming.poc;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client {

    /**
     * TRY TO TALK WITH CASSANDRA USING https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
     */

    public static void main(final String... args) throws InterruptedException {

        EventLoopGroup group = new NioEventLoopGroup(1);

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new CassandraRequestHandler());
                        }
                    });

            final ChannelFuture sync = b.connect("127.0.0.1", 9042).sync();

            Channel channel = sync.channel();
            ByteBufAllocator alloc = channel.alloc();

            ByteBuf buffer = alloc.heapBuffer();
            buffer.writeByte(0x04); //request
            buffer.writeBytes(new byte[]{0x00}); // flag
            buffer.writeBytes(new byte[]{0x00}); //stream id
            buffer.writeBytes(new byte[]{0x00}); //stream id
            buffer.writeBytes(new byte[]{0x01}); // startup

            channel.write(buffer);


            //body

            ByteBuf map = channel.alloc().buffer();

            String key = "CQL_VERSION";

            String value = "3.0.0";

            //map sie
            map.writeShort(1);

            //key
            map.writeShort(key.length());
            map.writeBytes(key.getBytes());

            //value
            map.writeShort(value.length());
            map.writeBytes(value.getBytes());


            ByteBuf bodySize = channel.alloc().buffer();

            short size = (short) (2 + 2 + key.length() + 2 + value.length());
            bodySize.writeInt(size);

            channel.write(bodySize);
            channel.writeAndFlush(map);
            channel.flush();

            channel.closeFuture().await();


        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }


    }
}
