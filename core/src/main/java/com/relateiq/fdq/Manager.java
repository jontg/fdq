package com.relateiq.fdq;

import com.foundationdb.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Created by mbessler on 2/25/15.
 */
public class Manager {
    public static final Logger log = LoggerFactory.getLogger(Manager.class);

    private final Database db;

    public Manager(Database db) {
        this.db = db;
    }

    /**
     * This will prevent the consumers from pulling any more message off the topic they are watching. The opposite of activate.
     *
     * @param topic
     */
    public void deactivate(String topic) {

    }

    /**
     * This will enable the consumers to pull messages off the topic they are watching. The opposite of deactivate.
     *
     * @param topic
     */
    public void activate(String topic) {

    }

    /**
     * Replay messages for a given period of time. If both minMillis and maxMillis are present, minMillis must be less or equal maxMillis
     *
     * @param topic     name of the topic
     * @param minMillis Optional min time in millis (inclusive)
     * @param maxMillis Optional max time in millis (exclusive)
     * @param direction Cursor ascending or descending?
     * @param limit     # of messages to replay
     */
    public void replay(String topic, OptionalLong minMillis, OptionalLong maxMillis, Direction direction, OptionalInt limit) {

    }

    public static enum Direction {
        ASCENDING,
        DESCENDING
    }
}
