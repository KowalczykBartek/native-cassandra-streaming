package com.directstreaming.poc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import static com.directstreaming.poc.CqlProtocolUtil.PAGE_STATE_MAGIC_NUMBER;
import static com.directstreaming.poc.CqlProtocolUtil.constructQueryMessage;

public class CassandraPartitionQueryHandler extends ChannelInboundHandlerAdapter {

    private long globalRowsCount = 0;

    private int queryNumber = 0;

    private ByteBuf byteBuf;

    private int rows;

    private int rowsIndex = 0;

    private byte[] page_state;

    private boolean finishMePleaseThereIsNoMoreResults = false;

    private final Channel requestingChannel;

    private final QueryManager queryManager;

    public CassandraPartitionQueryHandler(final ByteBuf byteBuf, final Channel requestingChannel, final QueryManager queryManager) {
        this.byteBuf = byteBuf;
        this.requestingChannel = requestingChannel;
        this.queryManager = queryManager;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {

        /**
         * FIXME// lets try to result this, because we are wasting lot of CPU cycles to allocate new direct buffer
         * FIXME// after each <page_state> query.
         *
         * FIXME// super important ! ensure that Buffers are released correctly !
         */
        byteBuf.writeBytes((ByteBuf) (msg));
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
            resetStateAndQueryBasedOnPageState(ctx);
        } else {
            queryNumber++;

        }

    }

    public void parseQueryResponseHeaders() {

        System.err.println("Version: " + byteBuf.readByte());
        System.err.println("Flag: " + byteBuf.readByte());
        System.err.println("StreamId: " + byteBuf.readByte() + "" + byteBuf.readByte());
        System.err.println("Op code: " + byteBuf.readByte());

        int size = byteBuf.readInt(); //BODY SIZE
        System.err.println("BODY SIZE: " + size);

        System.err.println("Response Type: " + byteBuf.readInt());

        int metadataFlag = byteBuf.readInt();

        System.err.println("metadataFlag: " + metadataFlag);

        if (metadataFlag == 5) {
            /**
             *  NO MORE PAGES AND NO <PAGING_STATE> INCLUDED
             */
            int columnsCount = byteBuf.readInt();
            System.err.println("columns count " + columnsCount);

            finishMePleaseThereIsNoMoreResults = true;

            rows = byteBuf.readInt();
            System.err.println("Rows count " + rows);
            System.err.println();


        } else {
            int columnsCount = byteBuf.readInt();
            System.err.println("columns count " + columnsCount);

            page_state = new byte[PAGE_STATE_MAGIC_NUMBER];
            byteBuf.readBytes(page_state);

            rows = byteBuf.readInt();
            System.err.println("Rows count " + rows);
            System.err.println();
        }

    }

    private void resetStateAndQueryBasedOnPageState(final ChannelHandlerContext ctx) {

        System.err.println("Processed rows from response " + rowsIndex);

        queryNumber = 0;

        byteBuf = null;

        rows = 0;

        rowsIndex = 0;

        byte[] page_state_temp = page_state;
        page_state = null;

        queryWithState(ctx, page_state_temp);
    }

    public void queryWithState(final ChannelHandlerContext ctx, byte[] page_state) {

        if (finishMePleaseThereIsNoMoreResults) {
            System.err.println("ALL PROCESSED ROWS " + globalRowsCount);


            if (queryManager.hasNextPartition()) {
                /**
                 * Install new handler and perform query for next partition.
                 */
                CassandraPartitionQueryUtil.installNewHandlerAndPerformQuery(ctx.channel(), requestingChannel, queryManager);

            } else {
                closeAndCleanupConnections(ctx);
            }

            return;
        }

        final ByteBuf buffer = ctx.alloc().heapBuffer();

        constructQueryMessage(buffer, queryManager.queryForCurrentPartition(), page_state);

        ctx.writeAndFlush(buffer);


        byteBuf = ctx.alloc().buffer();
    }

    private void closeAndCleanupConnections(final ChannelHandlerContext ctx) {
        ctx.channel().close().addListener(feature -> {
            if(!feature.isSuccess())
            {
                //FIXME log
            }
            requestingChannel.close();
        });
    }

    /**
     * <magic>
     * ( ͡° ͜ʖ ͡° )つ──☆*:・ﾟ
     * </magic>
     *
     * @param ctx
     */
    public void performRowRead(final ChannelHandlerContext ctx) {

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

                byte[] bytes = new byte[rowLength];

                byteBuf.readBytes(bytes);

//                final ByteBuf slice = byteBuf.slice(byteBuf.readerIndex(), rowLength);
//
//                byteBuf.readerIndex(byteBuf.readerIndex() + rowLength);

                //FIXME THIS HAVE TO BE REPLACED WITH NO-USER-SPACE COPY
                //FIXME BUT FOR END-TO-END, NOW IT CAN BE LIKE THAT.
                final ByteBuf buffer = ctx.alloc().buffer();
                buffer.writeBytes(bytes);

                //FIXME IS_WRITABLE HAVE TO BE CHECK, IF NO, THERE WILL BE NO BACKPRESSURE AT APPLICATION LEVEL.

                //flush, flush flush ....one performance killer more.

                if (requestingChannel != null) {
                    requestingChannel.writeAndFlush(buffer);
                }
                //FIXME use slice !
            }

            /*FIXME*/
            globalRowsCount++;
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }

}
