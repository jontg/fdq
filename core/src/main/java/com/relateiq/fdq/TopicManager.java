package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.relateiq.fdq.Helpers.toInt;
import static com.relateiq.fdq.Helpers.toLong;

/**
 * Created by mbessler on 2/25/15.
 */
public class TopicManager {
    public static final Logger log = LoggerFactory.getLogger(TopicManager.class);

    private final Database db;
    private final TopicConfig topicConfig;

    public TopicManager(Database db, TopicConfig topicConfig) {
        this.db = db;
        this.topicConfig = topicConfig;
    }

    public long runningCount() {
        return db.run((Function<Transaction, Integer>) tr -> tr.getRange(Range.startsWith(topicConfig.runningData.pack())).asList().get().size());
    }

    public TopicStats stats() {
        return db.run((Function<Transaction, TopicStats>) tr ->
                        new TopicStats(topicConfig.topic
                                , toInt(tr.get(topicConfig.topicMetricInserted()).get())
                                , toInt(tr.get(topicConfig.topicMetricAcked()).get())
                                , toInt(tr.get(topicConfig.topicMetricErrored()).get())
                                , toInt(tr.get(topicConfig.topicMetricPopped()).get())
                        )
        );
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
