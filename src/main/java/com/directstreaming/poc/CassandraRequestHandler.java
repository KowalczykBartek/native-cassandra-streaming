package com.directstreaming.poc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.UUID;

public class CassandraRequestHandler extends ChannelInboundHandlerAdapter {

    long globalRowsCount = 0;

    int queryNumber = 0;

    boolean firstTime = true;

    ByteBuf byteBuf;

    int rows;

    int rowsIndex = 0;

    byte[] page_state;

    boolean finishMePleaseThereIsNoMoreResults = false;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

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
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {


        if (queryNumber == 0) {
            ByteBuf buffer = ctx.alloc().heapBuffer();
            buffer.writeByte(0x04); //request

            buffer.writeBytes(new byte[]{0}); // flag

            buffer.writeBytes(new byte[]{0x01}); //stream id
            buffer.writeBytes(new byte[]{0x00}); //stream id
            buffer.writeBytes(new byte[]{0x07}); // query

            String query = "select * from YOUR_TABLE;";

            buffer.writeInt(query.length() + 4 + 2 + 1 + 4); //body size

            buffer.writeInt(query.length()); //size of query
            buffer.writeBytes(query.getBytes()); //query

            buffer.writeShort(0x0001);

            buffer.writeByte(0b0000_0110);
            buffer.writeInt(2000);

            ctx.writeAndFlush(buffer);

            byteBuf = ctx.alloc().buffer();

        } else if (queryNumber == 1) {

            asdasdasd();
            System.err.println("---------------" + rowsIndex + "------------------");

            if (rowsIndex == rows) {
                queryNumber = 0;

                firstTime = true;

                byteBuf = null;

                rows = 0;

                rowsIndex = 0;

                byte[] page_state_temp = page_state;
                page_state = null;

                queryWithState(ctx, page_state_temp);
            }

        } else {

            performRowRead();
            System.err.println("---------------" + rowsIndex + "------------------");

            if (rowsIndex == rows) {
                queryNumber = 0;

                firstTime = true;

                byteBuf = null;

                rows = 0;

                rowsIndex = 0;

                byte[] page_state_temp = page_state;
                page_state = null;

                queryWithState(ctx, page_state_temp);
            }

        }

        queryNumber++;


    }

    public void queryWithState(ChannelHandlerContext ctx, byte[] page_state) {

        if (finishMePleaseThereIsNoMoreResults) {
            System.err.println("ALL PROCESSED ROWS " + globalRowsCount);
            return;
        }

        ByteBuf buffer = ctx.alloc().heapBuffer();
        buffer.writeByte(0x04); //request

        buffer.writeBytes(new byte[]{0}); // flag

        buffer.writeBytes(new byte[]{0x01}); //stream id
        buffer.writeBytes(new byte[]{0x00}); //stream id
        buffer.writeBytes(new byte[]{0x07}); // query

        String query = "select * from YOUR_TABLE;";

        buffer.writeInt(query.length() + 4 + 2 + 1 + 4 + page_state.length); //body size

        buffer.writeInt(query.length()); //size of query
        buffer.writeBytes(query.getBytes()); //query

        buffer.writeShort(0x0001);

        buffer.writeByte(0b0000_1110);
        buffer.writeInt(2000);
        buffer.writeBytes(page_state);

        ctx.writeAndFlush(buffer);

        byteBuf = ctx.alloc().buffer();
    }

    public void asdasdasd() {

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

            //<col_spec_i>
            //read all columns and types
            System.err.println();

            finishMePleaseThereIsNoMoreResults = true;

            //rows count

            rows = byteBuf.readInt();
            System.err.println("Rows count " + rows);
            System.err.println();

            performRowRead();
        } else {
            int columnsCount = byteBuf.readInt();
            System.err.println("columns count " + columnsCount);

            page_state = new byte[63];
            byteBuf.readBytes(page_state);

//            System.err.println("page state start");
//            System.err.println();
//            for (int asdas = 0; asdas < 63; asdas++) {
//                System.err.println(page_state[asdas]);
//            }
//            System.err.println();
//            System.err.println("page state end");


            //<col_spec_i>
            //read all columns and types
            System.err.println();

            //rows count

            rows = byteBuf.readInt();
            System.err.println("Rows count " + rows);
            System.err.println();

            performRowRead();
        }

    }


    public void performRowRead() {

        for (; rowsIndex < rows; rowsIndex++) {

            int sumOfReadBytes = 0;

            //0
            {
                if (!byteBuf.isReadable(4)) {
                    return;
                }

                int rowLength = byteBuf.readInt();

                //FIXME "transaction"
                sumOfReadBytes += 4;

//                System.err.print("row [size: " + rowLength + "], type VARCHAR| ");

                if (!byteBuf.isReadable(rowLength)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                //FIXME "transaction"
                sumOfReadBytes += rowLength;

                byte[] content = new byte[rowLength];
                byteBuf.readBytes(content);


//                System.err.print(new String(content));

//                System.err.println();
            }
            //1
            {
                if (!byteBuf.isReadable(4)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                //FIXME "transaction"
                sumOfReadBytes += 4;
                int rowLength = byteBuf.readInt();

//                System.err.print("row [size: " + rowLength + "], type VARCHAR| ");

                if (!byteBuf.isReadable(rowLength)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                //FIXME "transaction"
                sumOfReadBytes += rowLength;

                byte[] content = new byte[rowLength];
                byteBuf.readBytes(content);

//                System.err.print(new String(content));
//
//                System.err.println();
            }
            //2
            {
                if (!byteBuf.isReadable(4)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                int rowLength = byteBuf.readInt();

                //FIXME "transaction"
                sumOfReadBytes += 4;

//                System.err.print("row [size: " + rowLength + "], type BIGINT| ");

                if (!byteBuf.isReadable(rowLength)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                long l = byteBuf.readLong();
//                    System.err.print(byteBuf.readLong());
                //FIXME "transaction"
                sumOfReadBytes += 8;

//                    System.err.println();
            }
            //3
            {
                if (!byteBuf.isReadable(4)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                int rowLength = byteBuf.readInt();
                //FIXME "transaction"
                sumOfReadBytes += 4;

//                System.err.print("row [size: " + rowLength + "], type UUID| ");

                if (!byteBuf.isReadable(8)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }
                long mostSignificant = byteBuf.readLong();
                long lessSignificant = byteBuf.readLong();

                //FIXME "transaction"
                sumOfReadBytes += 16;

                UUID uuid = new UUID(mostSignificant, lessSignificant);

//                System.err.print(uuid.toString());
//                System.err.println();
            }
            //4
            {
                if (!byteBuf.isReadable(4)) {
                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }
                int rowLength = byteBuf.readInt();
                //FIXME "transaction"
                sumOfReadBytes += 4;

//                System.err.print("row [size: " + rowLength + "], type BIGINT| ");

                if (!byteBuf.isReadable(rowLength)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }
                long l = byteBuf.readLong();

//                System.err.print(byteBuf.readLong());
                //FIXME "transaction"
                sumOfReadBytes += 8;

//                    System.err.println();
            }

            //5
            {
                if (!byteBuf.isReadable(4)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }

                int rowLength = byteBuf.readInt();

                //FIXME "transaction"
                sumOfReadBytes += 4;

//                System.err.print("row [size: " + rowLength + "], type BIGINT| ");

                if (!byteBuf.isReadable(rowLength)) {

                    byteBuf.readerIndex(byteBuf.readerIndex() - sumOfReadBytes);

                    return;
                }
                long l = byteBuf.readLong();
//                    System.err.print(byteBuf.readLong());
                //FIXME "transaction"
                sumOfReadBytes += 8;

//                    System.err.println();
            }
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
