package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.relateiq.fdq.Helpers.DEACTIVATED;
import static com.relateiq.fdq.Helpers.rmdir;
import static com.relateiq.fdq.Helpers.toInt;
import static com.relateiq.fdq.TopicConfig.*;

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
                                , Helpers.fetchAssignments(tr, topicConfig.assignments).asMap()
                                , metric(tr, METRIC_INSERTED)
                                , metric(tr, METRIC_ACKED)
                                , metric(tr, METRIC_ACKED_DURATION)
                                , metric(tr, METRIC_ERRORED)
                                , metric(tr, METRIC_ERRORED_DURATION)
                                , metric(tr, METRIC_TIMED_OUT)
                                , metric(tr, METRIC_POPPED)
                        )
        );
    }

    private int metric(Transaction tr, String metricName) {
        return toInt(tr.get(topicConfig.metric(metricName)).get());
    }

    /**
     * This will prevent the consumers from pulling any more message off the topic they are watching. The opposite of activate.
     */
    public void deactivate() {
        db.run((Function<Transaction, Void>) tr -> {tr.set(topicConfig.config.pack(DEACTIVATED), Helpers.NULL); return null;});
    }

    /**
     * This will enable the consumers to pull messages off the topic they are watching. The opposite of deactivate.
     */
    public void activate() {
        db.run((Function<Transaction, Void>) tr -> {tr.clear(topicConfig.config.pack(DEACTIVATED)); return null;});
    }

    public boolean isActivated() {
        return db.run((Function<Transaction, Boolean>) tr ->
                Helpers.isActivated(tr, topicConfig));
    }


    /**
     * Replay messages for a given period of time. If both minMillis and maxMillis are present, minMillis must be less or equal maxMillis
     *
     * @param minMillis Optional min time in millis (inclusive)
     * @param maxMillis Optional max time in millis (exclusive)
     * @param direction Cursor ascending or descending?
     * @param limit     # of messages to replay
     */
    public void replay(OptionalLong minMillis, OptionalLong maxMillis, Direction direction, OptionalInt limit) {

    }

    /**
     * This will remove all configuration and data information about a topic. PERMANENTLY!
     * <p>
     * BE CAREFUL
     */
    public void nuke() {
        rmdir(db, topicConfig.topic);
    }

    public static enum Direction {
        ASCENDING,
        DESCENDING
    }
}
