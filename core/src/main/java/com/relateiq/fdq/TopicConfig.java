package com.relateiq.fdq;

import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Created by mbessler on 2/23/15.
 */
public class TopicConfig {

    public static final int DEFAULT_NUM_SHARDS = 24;  // todo: make this configurable

    public static final String METRIC_INSERTED = "inserted";
    public static final String METRIC_POPPED = "popped";
    public static final String METRIC_ACKED = "acked";
    public static final String METRIC_ERRORED = "errored";
    private static final String METRIC_SKIPPED = "skipped";

    public final String topic;

    public final DirectorySubspace assignments;
    public final DirectorySubspace heartbeats;
    public final DirectorySubspace topicMetrics;

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


    public TopicConfig(String topic, DirectorySubspace assignments, DirectorySubspace heartbeats, DirectorySubspace topicMetrics, DirectorySubspace runningData, DirectorySubspace runningShardKeys, Map<Integer, DirectorySubspace> shardMetrics, Map<Integer, DirectorySubspace> shardData) {
        this.topic = topic;
        this.assignments = assignments;
        this.heartbeats = heartbeats;
        this.topicMetrics = topicMetrics;
        this.runningShardKeys = runningShardKeys;
        this.shardMetrics = shardMetrics;
        this.shardData = shardData;
        this.runningData = runningData;
    }

    public byte[] shardMetricInserted(Integer shardIndex) {
        return shardMetrics.get(shardIndex).pack(Tuple.from(METRIC_INSERTED));
    }

    public byte[] shardMetricAcked(Integer shardIndex) {
        return shardMetrics.get(shardIndex).pack(Tuple.from(METRIC_ACKED));
    }

    public byte[] shardMetricErrored(Integer shardIndex) {
        return shardMetrics.get(shardIndex).pack(Tuple.from(METRIC_ERRORED));
    }

    public byte[] shardMetricPopped(Integer shardIndex) {
        return shardMetrics.get(shardIndex).pack(Tuple.from(METRIC_POPPED));
    }

    public byte[] shardMetricSkipped(Integer shardIndex) {
        return shardMetrics.get(shardIndex).pack(Tuple.from(METRIC_SKIPPED));
    }

    public byte[] topicMetricInserted() {
        return topicMetrics.pack(Tuple.from(METRIC_INSERTED));
    }

    public byte[] topicMetricAcked() {
        return topicMetrics.pack(Tuple.from(METRIC_ACKED));
    }

    public byte[] topicMetricErrored() {
        return topicMetrics.pack(Tuple.from(METRIC_ERRORED));
    }

    public byte[] topicMetricSkipped() {
        return topicMetrics.pack(Tuple.from(METRIC_SKIPPED));
    }

    public byte[] topicMetricPopped() {
        return topicMetrics.pack(Tuple.from(METRIC_POPPED));
    }

    public byte[] getTopicAssignmentsKey(Integer shardIndex) {
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
}
