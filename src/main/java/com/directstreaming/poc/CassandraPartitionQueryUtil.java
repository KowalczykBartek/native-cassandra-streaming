package com.directstreaming.poc;


import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

import static com.directstreaming.poc.CqlProtocolUtil.constructQueryMessage;

public class CassandraPartitionQueryUtil {

    private static final String CASSANDRA_PARTITION_HANDLER_NAME = "cassandraPartitionHandler";

    /**
     * fixme
     */
    public static void installNewHandlerAndPerformQuery(final Channel cassandraChannel, final Channel requestingChannel, final QueryManager queryManager) {
        //perform "garbage collection".
        final ChannelHandler channelHandlerToRemove =
                cassandraChannel.pipeline().get(CASSANDRA_PARTITION_HANDLER_NAME);

        if (channelHandlerToRemove != null) {
            cassandraChannel.pipeline().remove(CASSANDRA_PARTITION_HANDLER_NAME);
        }

        //new fresh handler.
        final ByteBuf responseHandlerBuffer = cassandraChannel.alloc().buffer();
        cassandraChannel.pipeline().addLast(CASSANDRA_PARTITION_HANDLER_NAME, new CassandraPartitionQueryHandler(responseHandlerBuffer, requestingChannel, queryManager));

        final ByteBuf buffer = cassandraChannel.alloc().heapBuffer();
        constructQueryMessage(buffer, queryManager.queryForNewPartition(), null);
        cassandraChannel.writeAndFlush(buffer);
    }

}
