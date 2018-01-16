package com.directstreaming.poc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

import static com.directstreaming.poc.CqlProtocolUtil.PAGE_STATE_MAGIC_NUMBER;
import static com.directstreaming.poc.CqlProtocolUtil.constructQueryMessage;

public class CassandraPartitionQueryHandler extends ChannelInboundHandlerAdapter {

    private static Logger LOG = Logger.getLogger(CassandraPartitionQueryHandler.class);

    private boolean wasWritingStopped = false;

    private long globalRowsCount = 0;

    private int queryNumber = 0;

    private ByteBuf byteBuf;

    private int rows;

    private int rowsIndex = 0;

    private byte[] page_state;

    private boolean finishMePleaseThereIsNoMoreResults = false;

    private final StreamingBridge bridge;

    private final QueryManager queryManager;

    private ChannelHandlerContext ctx;

    public CassandraPartitionQueryHandler(final ByteBuf byteBuf, final StreamingBridge bridge, final QueryManager queryManager) {
        this.byteBuf = byteBuf;
        this.bridge = bridge;
        this.queryManager = queryManager;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        LOG.info("Going to unregister and releasing bytebuf " + byteBuf);
        byteBuf.release();
        LOG.info("Ref count of " + byteBuf + " is " + byteBuf.refCnt());
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {

        if (this.ctx == null) {
            this.ctx = ctx;
        }

        ByteBuf receivedMessage = null;
        try {
            receivedMessage = (ByteBuf) (msg);

            byteBuf.discardReadBytes();
            byteBuf.writeBytes(receivedMessage);
        } finally {
            /*
             * see http://netty.io/wiki/reference-counted-objects.html
             */
            if (receivedMessage != null) {
                receivedMessage.release();
            }
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {

        if (queryNumber == 0) {

            parseQueryResponseHeaders();

            performRowRead(ctx);

        } else {
            performRowRead(ctx);
        }

        if (rowsIndex == rows) {

            bridge.pokeMe.run();

        } else {
            queryNumber++;

        }

    }

    public void continueReading() {
        if (ctx.executor().inEventLoop()) {
            resetStateAndQueryBasedOnPageState(ctx);
        } else {
            ctx.executor().execute(() -> {
                resetStateAndQueryBasedOnPageState(ctx);
            });
        }
    }

    public void parseQueryResponseHeaders() {

        byteBuf.readerIndex(byteBuf.readerIndex() + 1);

        byteBuf.readerIndex(byteBuf.readerIndex() + 1);

        byteBuf.readerIndex(byteBuf.readerIndex() + 2);

        byteBuf.readerIndex(byteBuf.readerIndex() + 1);

        byteBuf.readerIndex(byteBuf.readerIndex() + 4);

        byteBuf.readerIndex(byteBuf.readerIndex() + 4);

        int metadataFlag = byteBuf.readInt();

        if (metadataFlag == 5) {
            /**
             *  NO MORE PAGES AND NO <PAGING_STATE> INCLUDED
             */

            byteBuf.readerIndex(byteBuf.readerIndex() + 4);

            finishMePleaseThereIsNoMoreResults = true;

            rows = byteBuf.readInt();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Rows count " + rows);
            }

        } else {
            byteBuf.readerIndex(byteBuf.readerIndex() + 4);

            page_state = new byte[PAGE_STATE_MAGIC_NUMBER];
            byteBuf.readBytes(page_state);

            rows = byteBuf.readInt();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Rows count " + rows);
            }
        }

    }

    private void resetStateAndQueryBasedOnPageState(final ChannelHandlerContext ctx) {
        LOG.info("Processed rows from response " + rowsIndex);

        queryNumber = 0;

        rows = 0;

        rowsIndex = 0;

        wasWritingStopped = false;

        byte[] page_state_temp = page_state;
        page_state = null;

        queryWithState(ctx, page_state_temp);
    }

    public void queryWithState(final ChannelHandlerContext ctx, byte[] page_state) {

        if (finishMePleaseThereIsNoMoreResults) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("All rows processed " + globalRowsCount);
            }

            if (queryManager.hasNextPartition()) {
                /**
                 * Install new handler and perform query for next partition.
                 */
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Starting new query");
                }
                //this byteBuf will be reused
                byteBuf.release();
                byteBuf = ctx.alloc().directBuffer();

                CassandraPartitionQueryUtil.installNewHandlerAndPerformQuery(byteBuf, ctx.channel(), bridge, queryManager);

            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Closing connection");
                }

                closeAndCleanupConnections(ctx);
            }

            return;
        }

        final ByteBuf buffer = ctx.alloc().directBuffer();

        constructQueryMessage(buffer, queryManager.queryForCurrentPartition(), page_state);

        ctx.writeAndFlush(buffer);

        byteBuf.release();
        byteBuf = ctx.alloc().directBuffer();
    }

    private void closeAndCleanupConnections(final ChannelHandlerContext ctx) {
        ctx.channel().close().addListener(feature -> {
            if (!feature.isSuccess()) {
                LOG.error("Exception ", feature.cause());
            }

            ctx.channel().close();
            bridge.cleanupIncoming.run();
        });
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable() && wasWritingStopped) {
            performRowRead(ctx);

            wasWritingStopped = false;
        }
    }


    /**
     * <magic>
     * ( ͡° ͜ʖ ͡° )つ──☆*:・ﾟ
     * </magic>
     *
     * @param ctx
     */
    public void performRowRead(final ChannelHandlerContext ctx) {

        /**
         * Backpressure is super important - we have to stop write to channel if Netty cannot keep up, because,
         * if we will not do that, GC will be suffered. Ff writability will change its state to "writable"
         * we will again start writing - check channelWritabilityChanged
         */
        if (!ctx.channel().isWritable()) {
            LOG.info("Write have to be postponed");

            wasWritingStopped = true;

            return;
        }

        for (; rowsIndex < rows; rowsIndex++) {

            int sumOfReadBytes = 0;

            {
                if (!byteBuf.isReadable(4)) {
                    return;
                }

                int rowLength = byteBuf.readInt();
                sumOfReadBytes += 4;

                if (!byteBuf.isReadable(rowLength)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }
                sumOfReadBytes += rowLength;

                byte[] content = new byte[rowLength];
                byteBuf.readBytes(content);
            }
            {
                if (!byteBuf.isReadable(4)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                sumOfReadBytes += 4;
                int rowLength = byteBuf.readInt();

                if (!byteBuf.isReadable(rowLength)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }
                sumOfReadBytes += rowLength;

                byte[] content = new byte[rowLength];
                byteBuf.readBytes(content);
            }
            {
                if (!byteBuf.isReadable(4)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                byteBuf.readInt(); //size
                sumOfReadBytes += 4;

                if (!byteBuf.isReadable(8)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                long l = byteBuf.readLong();

                sumOfReadBytes += 8;
            }
            {
                if (!byteBuf.isReadable(4)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }
                byteBuf.readInt();
                sumOfReadBytes += 4;

                if (!byteBuf.isReadable(8)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                long l = byteBuf.readLong();

                sumOfReadBytes += 8;
            }
            {
                if (!byteBuf.isReadable(4)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                sumOfReadBytes += 4;
                int rowLength = byteBuf.readInt();

                if (!byteBuf.isReadable(rowLength)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }
                sumOfReadBytes += rowLength;

                //FIXME I BELIEVE !!!
                bridge.interThreadBuffer.writeBytes(byteBuf, rowLength);

            }

            globalRowsCount++;
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        // Close the connection when an exception is raised.
        LOG.error("Exception occurred", cause);
        ctx.close();
    }

}
