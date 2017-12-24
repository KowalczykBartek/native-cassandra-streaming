package com.directstreaming.poc;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.UUID;

import static com.directstreaming.poc.CqlProtocolUtil.PAGE_STATE_MAGIC_NUMBER;
import static com.directstreaming.poc.CqlProtocolUtil.constructQueryMessage;

public class CassandraRequestHandler extends ChannelInboundHandlerAdapter {

    private long globalRowsCount = 0;

    private int queryNumber = 0;

    private boolean firstTime = true;

    private ByteBuf byteBuf;

    private int rows;

    private int rowsIndex = 0;

    private byte[] page_state;

    private boolean finishMePleaseThereIsNoMoreResults = false;

    private final Channel requestingChannel;

    public CassandraRequestHandler(final Channel requestingChannel) {
        this.requestingChannel = requestingChannel;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        /**
         * FIXME// lets try to result this, because we are wasting lot of CPU cycles to alocate new direct buffer
         * FIXME// after each <page_state> query.
         */
        /**
         * FIXME// super important ! ensure that Buffers are released correctly !
         */
        if (queryNumber == 1) {
            if (firstTime) {
                firstTime = false;
                byteBuf = ctx.channel().alloc().buffer();
            }

            byteBuf.writeBytes((ByteBuf) (msg));
        } else if (queryNumber > 1) {
            byteBuf.writeBytes((ByteBuf) (msg));
        }
    }

    public static int unsignedToBytes(byte b) {
        return b & 0xFF;
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {


        if (queryNumber == 0) {
            final ByteBuf buffer = ctx.alloc().heapBuffer();

            constructQueryMessage(buffer, null);

            ctx.writeAndFlush(buffer);

            byteBuf = ctx.alloc().buffer();

        } else if (queryNumber == 1) {

            parseQueryResponseHeaders();

            performRowRead(ctx);

        } else {

            performRowRead(ctx);
        }

        queryNumber++;
    }

    private void resetStateAndQueryBasedOnPageState(final ChannelHandlerContext ctx) {

        System.err.println("Processed rows from response " + rowsIndex);

        queryNumber = 0;

        firstTime = true;

        byteBuf = null;

        rows = 0;

        rowsIndex = 0;

        byte[] page_state_temp = page_state;
        page_state = null;

        queryWithState(ctx, page_state_temp);
    }

    public void queryWithState(ChannelHandlerContext ctx, byte[] page_state) {

        if (finishMePleaseThereIsNoMoreResults) {
            System.err.println("ALL PROCESSED ROWS " + globalRowsCount);

            requestingChannel.close();//FIXME don't forget about me.

            return;
        }

        final ByteBuf buffer = ctx.alloc().heapBuffer();

        constructQueryMessage(buffer, page_state);

        ctx.writeAndFlush(buffer);

        byteBuf = ctx.alloc().buffer();
    }

    public void parseQueryResponseHeaders() {

        System.err.println("Version: " + unsignedToBytes(byteBuf.readByte()));
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

                int rowLength = byteBuf.readInt();

                sumOfReadBytes += 4;

                if (!byteBuf.isReadable(rowLength)) {

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

                int rowLength = byteBuf.readInt();
                sumOfReadBytes += 4;

                if (!byteBuf.isReadable(8)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }
                long mostSignificant = byteBuf.readLong();
                long lessSignificant = byteBuf.readLong();

                sumOfReadBytes += 16;

                UUID uuid = new UUID(mostSignificant, lessSignificant);
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

                //FIXME ISWRITABLE HAVE TO BE CHECK, IF NO, THERE WILL BE NO BACKPRESSURE AT APPLICATION LEVEL.

                //flush, flush flush ....one performance killer more.

                if (requestingChannel != null) {
                    requestingChannel.writeAndFlush(buffer);
                }
                //FIXME use slice !
            }

            /*FIXME*/
            globalRowsCount++;
        }

        if (rowsIndex == rows) {
            resetStateAndQueryBasedOnPageState(ctx);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
