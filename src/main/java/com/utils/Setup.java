package com.utils;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;

import java.util.Random;

/**
 *
 */
public class Setup {

    private static final String KEYSPACE = "test_keyspace";
    private static final String TABLE = "test_table";

    private static final String CELL_NAME_1 = "cell1";
    private static final String CELL_NAME_2 = "cell2";
    private static final String CELL_NAME_3 = "cell3";
    private static final String CELL_NAME_4 = "cell4";
    private static final String CELL_NAME_5 = "cell5";
    private static final String CELL_NAME_6 = "cell6";

    private static final String INSERT_QUERY = "INSERT INTO " + KEYSPACE + "." + TABLE + //
            " (" + CELL_NAME_1 + "," + CELL_NAME_2 + "," + CELL_NAME_3 + "," + CELL_NAME_4 + "," +
            CELL_NAME_5 + "," + CELL_NAME_6 + ") values (?, ?, ?, ?,?,?)";

    public final static String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS " + //
            KEYSPACE + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

    private static final String CREATE_TABLE_QUERY = //
            "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE + "(" + //
                    CELL_NAME_1 + " text," + //
                    CELL_NAME_2 + " text, " + //
                    CELL_NAME_3 + " bigint, " + //
                    CELL_NAME_4 + " uuid, " + //
                    CELL_NAME_5 + " bigint, " + //
                    CELL_NAME_6 + " bigint, " + //
                    "PRIMARY KEY ((" + CELL_NAME_1 + "," + CELL_NAME_2 + "," + CELL_NAME_3 + ")," //
                    /* clustering column */ + CELL_NAME_4 + "));";


    public static void main(final String... args) {
        final QueryLogger queryLogger = QueryLogger.builder()
                .build();

        final Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .build();

        cluster.register(queryLogger);

        final Session session = cluster.connect();


        session.execute(CREATE_KEYSPACE);
        session.execute(CREATE_TABLE_QUERY);

        final PreparedStatement insertExtensionMetrics =  //
                session.prepare(INSERT_QUERY);

        long bucket = bucketize(1111811051321l);

        while (true) {
            bucket += 1;
            final BoundStatement preparedInsert = insertExtensionMetrics.//
                    bind("value1", "value2", bucket, UUIDs.timeBased(),
                    (new Random().nextInt(2500) + 1) + 1l, (new Random().nextInt(2500) + 1) + 1l);


            session.execute(preparedInsert.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));

            System.err.println("Query finished");
        }
    }

    public static long bucketize(final long timestamp) {
        return (timestamp / 60_000) * 60_000;
    }

}
