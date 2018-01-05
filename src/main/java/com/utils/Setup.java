package com.utils;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;
import com.utils.client.SimpleStreamingClientHandler;
import org.apache.log4j.Logger;

import java.util.Random;

/**
 *
 */
public class Setup {

    static Logger LOG = Logger.getLogger(Setup.class);

    private static final String EXAMPLE_JSON = "{" +
            "\"id\": 1," +
            "\"name\": \"A green door\"," +
            "\"price\": 12.50," +
            "\"tags\": [\"home\", \"green\"]" +
            "}";

    private static final String KEYSPACE = "test_keyspace";
    private static final String TABLE = "test_table";

    private static final String CELL_NAME_1 = "cell1";
    private static final String CELL_NAME_2 = "cell2";
    private static final String CELL_NAME_3 = "cell3";
    private static final String CELL_NAME_4 = "cell4";
    private static final String CELL_NAME_5 = "cell5";

    private static final String INSERT_QUERY = "INSERT INTO " + KEYSPACE + "." + TABLE + //
            " (" + CELL_NAME_1 + "," + CELL_NAME_2 + "," + CELL_NAME_3 + "," + CELL_NAME_4 + "," +
            CELL_NAME_5 + ") values (?, ?, ?, ?, ?)";

    public final static String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS " + //
            KEYSPACE + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

    private static final String CREATE_TABLE_QUERY = //
            "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE + "(" + //
                    CELL_NAME_1 + " text," + //
                    CELL_NAME_2 + " text, " + //
                    CELL_NAME_3 + " bigint, " + //
                    CELL_NAME_4 + " bigint, " + //
                    CELL_NAME_5 + " text, " + //
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

        long bucket = 1;

        while (true) {

            for (long i = 0; i < 50000; i++) {

                final BoundStatement preparedInsert = insertExtensionMetrics.//
                        bind("value1", "value2", bucket, i, EXAMPLE_JSON);

                session.execute(preparedInsert.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));

            }

            LOG.info("Query finished for " + bucket);

            bucket++;
        }
    }

}
