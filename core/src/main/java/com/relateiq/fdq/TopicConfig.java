package com.relateiq.fdq;

import com.foundationdb.MutationType;
import com.foundationdb.Transaction;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.Tuple;

import java.util.Map;

import static com.relateiq.fdq.Helpers.ONE;
import static com.relateiq.fdq.Helpers.intToByteArray;

/**
 * Created by mbessler on 2/23/15.
 */
public class TopicConfig {

    public static final int DEFAULT_NUM_SHARDS = 24;  // todo: make this configurable

    public static final String METRIC_INSERTED = "inserted";
    public static final String METRIC_POPPED = "popped";
    public static final String METRIC_ACKED = "acked";
    public static final String METRIC_ERRORED = "errored";
    public static final String METRIC_SKIPPED = "skipped";
    public static final String METRIC_TIMED_OUT = "timedOut";
    public static final String METRIC_ERRORED_DURATION = "erroredDuration";
    public static final String METRIC_ACKED_DURATION = "ackedDuration";

    public final String topic;

    public final DirectorySubspace assignments;
    public final DirectorySubspace heartbeats;
    public final DirectorySubspace topicMetrics;


    public final DirectorySubspace erroredData;

    /**
     * To prevent losing messages that have been popped but not acked we track which are currently running here
     * where the value is the pop time
     */
    public final DirectorySubspace runningData;

    /**
     * To prevent 2 messages with same shard key from running concurrently on multiple consumers/threads
     * we keep track of which shard keys are currently running.
     */
    public final DirectorySubspace runningShardKeys;

    public final Map<Integer, DirectorySubspace> shardMetrics;
    public final Map<Integer, DirectorySubspace> shardData;
    public final int numShards = DEFAULT_NUM_SHARDS;


    public TopicConfig(String topic, DirectorySubspace assignments, DirectorySubspace heartbeats, DirectorySubspace topicMetrics, DirectorySubspace erroredData, DirectorySubspace runningData, DirectorySubspace runningShardKeys, Map<Integer, DirectorySubspace> shardMetrics, Map<Integer, DirectorySubspace> shardData) {
        this.topic = topic;
        this.assignments = assignments;
        this.heartbeats = heartbeats;
        this.topicMetrics = topicMetrics;
        this.erroredData = erroredData;
        this.runningShardKeys = runningShardKeys;
        this.shardMetrics = shardMetrics;
        this.shardData = shardData;
        this.runningData = runningData;
    }

    public byte[] shardMetric(Integer shardIndex, String metricName) {
        return shardMetrics.get(shardIndex).pack(Tuple.from(metricName));
    }

    public byte[] metricInserted() {
        return topicMetrics.pack(Tuple.from(METRIC_INSERTED));
    }

    public byte[] metricAcked() {
        return metric(METRIC_ACKED);
    }

    public byte[] metric(String metricName) {
        return topicMetrics.pack(Tuple.from(metricName));
    }

    public byte[] metricAckedDuration() {
        return topicMetrics.pack(Tuple.from(METRIC_ACKED_DURATION));
    }

    public byte[] metricErrored() {
        return topicMetrics.pack(Tuple.from(METRIC_ERRORED));
    }

    public byte[] metricErroredDuration() {
        return topicMetrics.pack(Tuple.from(METRIC_ERRORED_DURATION));
    }

    public byte[] metricSkipped() {
        return topicMetrics.pack(Tuple.from(METRIC_SKIPPED));
    }

    public byte[] metricPopped() {
        return topicMetrics.pack(Tuple.from(METRIC_POPPED));
    }

    public byte[] shardAssignmentKey(Integer shardIndex) {
        return assignments.pack(shardIndex);
    }

    @Override
    public String toString() {
        return "{" +
                "'topic' : '" + topic + '\'' +
                '}';
    }

    byte[] messageKey(Integer shardIndex, long insertionTime, int randomInt) {
        return shardData.get(shardIndex).pack(Tuple.from(insertionTime, randomInt));
    }

    public void incShardMetric(Transaction tr, int shardIndex, String metricName) {
        tr.mutate(MutationType.ADD, shardMetric(shardIndex, metricName), ONE);
    }

    public void incShardMetric(Transaction tr, int shardIndex, String metricName, int amount) {
        tr.mutate(MutationType.ADD, shardMetric(shardIndex, metricName), intToByteArray(amount));
    }

    public void incMetric(Transaction tr, String metricName) {
        tr.mutate(MutationType.ADD, metric(metricName), ONE);
    }

    public void incMetric(Transaction tr, String metricName, int amount) {
        tr.mutate(MutationType.ADD, metric(metricName), intToByteArray(amount));
    }
}
