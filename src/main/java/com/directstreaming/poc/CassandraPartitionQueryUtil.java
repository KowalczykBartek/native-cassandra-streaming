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
    public static void installNewHandlerAndPerformQuery(final ByteBuf responseHandlerBuffer, final Channel cassandraChannel, final StreamingBridge bridge,
                                                        final QueryManager queryManager) {
        //perform "garbage collection".
        final ChannelHandler channelHandlerToRemove =
                cassandraChannel.pipeline().get(CASSANDRA_PARTITION_HANDLER_NAME);

        if (channelHandlerToRemove != null) {

            cassandraChannel.pipeline().remove(CASSANDRA_PARTITION_HANDLER_NAME);
        }

        //new fresh handler.
        CassandraPartitionQueryHandler cassandraPartitionQueryHandler;

        if (responseHandlerBuffer == null) {
            final ByteBuf newResponseHandlerBuffer = cassandraChannel.alloc().directBuffer();

            cassandraPartitionQueryHandler =
                    new CassandraPartitionQueryHandler(newResponseHandlerBuffer, bridge, queryManager);
        } else {
            cassandraPartitionQueryHandler =
                    new CassandraPartitionQueryHandler(responseHandlerBuffer, bridge, queryManager);
        }

        cassandraChannel.pipeline().addLast(CASSANDRA_PARTITION_HANDLER_NAME, cassandraPartitionQueryHandler);

        /**
         * Lets reset bridge - this is not best place because it makes responsibility more distributed,
         * but I want it working ASAP.
         */
        bridge.interThreadBuffer.clear();

        bridge.continueReading = () -> {
            cassandraPartitionQueryHandler.continueReading();
        };

        /*
         * Construct first query that will be later handler by cassandraPartitionQueryHandler
         */
        final ByteBuf buffer = cassandraChannel.alloc().directBuffer();
        constructQueryMessage(buffer, queryManager.queryForNewPartition(), null);
        cassandraChannel.writeAndFlush(buffer);
    }

}
