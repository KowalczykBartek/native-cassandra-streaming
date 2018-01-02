package com.directstreaming.poc;

import io.netty.channel.Channel;

import java.util.concurrent.atomic.LongAdder;

public class QueryManager {

    private static final String QUERY =
            "SELECT * FROM test_keyspace.test_table where cell1='value1' and cell2='value2' and cell3=%s;";

    private final LongAdder counter;

    public QueryManager() {
        this.counter = new LongAdder();
    }

    public String queryForNewPartition() {

        counter.increment();

        final int value = counter.intValue();

        final String finalQuery = String.format(QUERY, value);

        return finalQuery;
    }

    public String queryForCurrentPartition()
    {
        final int value = counter.intValue();

        final String finalQuery = String.format(QUERY, value);

        return finalQuery;
    }

    public boolean hasNextPartition()
    {
        return counter.intValue() < 10;
    }
}
