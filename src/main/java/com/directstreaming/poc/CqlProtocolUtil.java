package com.directstreaming.poc;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicInteger;

public class CqlProtocolUtil {

    /**
     * I will be honest with you - I could not find info about <page_state> in
     * https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec so I just looked at Cassandra
     * source code, and it seems that ResultSet.java:409 (pagingState.serialize(version)) writes X bytes,
     * so I just hardcoded it for now - but of course FIXME ;D
     *
     * FIXME This value depends on query !
     */
    public static final int PAGE_STATE_MAGIC_NUMBER = 26;

    private final static String CQL_VERSION_KEY = "CQL_VERSION";
    private final static String CQL_VERSION_VALUE = "3.0.0";

    private static final AtomicInteger root = new AtomicInteger();

    public static short incrementAndGet()
    {
        return (short) (root.incrementAndGet() % Short.MAX_VALUE);
    }

    /**
     * Construct STARTUP message - it will allows for further interactions with Cassandra.
     *
     * @param buffer
     */
    public static void constructStartupMessage(final ByteBuf buffer) {
        /*
         *  Requests headers - according to https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
         *  section #1
         *
         *  0         8        16        24        32         40
         *  +---------+---------+---------+---------+---------+
         *  | version |  flags  |      stream       | opcode  |
         *  +---------+---------+---------+---------+---------+
         */

        buffer.writeByte(0x04); //request
        buffer.writeBytes(new byte[]{0x00}); // flag
        buffer.writeShort(incrementAndGet()); //stream id
        buffer.writeBytes(new byte[]{0x01}); // startup

        /*
         * Body size - int - 4bytes.
         * Body of STARTUP message contains MAP of 1 entry, where key and value is [string]
         *
         * Size = (map size) + (key size) + (key string length) + (value size) + (value string length)
         */
        short size = (short) (2 + 2 + CQL_VERSION_KEY.length() + 2 + CQL_VERSION_VALUE.length());

        buffer.writeInt(size);

        /*
         * Write body itself, lets say that is it Map.of("CQL_VERSION","3.0.0") in CQL version.
         */

        //map sie
        buffer.writeShort(1);

        //key
        buffer.writeShort(CQL_VERSION_KEY.length());
        buffer.writeBytes(CQL_VERSION_KEY.getBytes());

        //value
        buffer.writeShort(CQL_VERSION_VALUE.length());
        buffer.writeBytes(CQL_VERSION_VALUE.getBytes());
    }

    /**
     * Construct query message based on {@param page_state} param - it present, will be encoded, if not
     * will be ignored.
     *
     * @param buffer
     * @param page_state
     */
    public static void constructQueryMessage(final ByteBuf buffer, final String queryMessage, final byte[] page_state) {
        buffer.writeByte(0x04); //request

        buffer.writeBytes(new byte[]{0}); // flag

        buffer.writeShort(incrementAndGet()); //stream id

        buffer.writeBytes(new byte[]{0x07}); // query

        if (page_state != null) {
            buffer.writeInt(queryMessage.length() + 4 + 2 + 1 + 4 + page_state.length); //body size
        } else {
            buffer.writeInt(queryMessage.length() + 4 + 2 + 1 + 4); //body size
        }

        buffer.writeInt(queryMessage.length()); //size of query
        buffer.writeBytes(queryMessage.getBytes()); //query

        //consistency 0x0001 == ONE, fixme later maybe
        buffer.writeShort(0x0001);

        /*
         * Section 4.1.4 from https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
         */
        if (page_state != null) {
            buffer.writeByte(0b0000_1110);
        } else {
            buffer.writeByte(0b0000_0110);
        }

        buffer.writeInt(500);

        if (page_state != null) {
            buffer.writeBytes(page_state);
        }
    }

}
