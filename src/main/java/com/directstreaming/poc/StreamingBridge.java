package com.directstreaming.poc;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * FIXME docu
 */
public class StreamingBridge {

    final ByteBuf interThreadBuffer;

    public StreamingBridge(final ByteBuf interThreadBuffer)
    {
        this.interThreadBuffer = interThreadBuffer;
    }

    public volatile Runnable pokeMe;

    public volatile Runnable continueReading;

    public volatile Runnable cleanupIncoming;
}
