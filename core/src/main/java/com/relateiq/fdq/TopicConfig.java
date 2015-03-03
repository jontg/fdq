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

    public final String topic;

    public final DirectorySubspace assignments;
    public final DirectorySubspace heartbeats;
    public final DirectorySubspace runningData;
    public final DirectorySubspace runningShardKeys;
    public final Map<Integer, DirectorySubspace> shardMetrics;
    public final Map<Integer, DirectorySubspace> shardData;
    public final int numShards = DEFAULT_NUM_SHARDS;


    public TopicConfig(String topic, DirectorySubspace assignments, DirectorySubspace heartbeats, DirectorySubspace runningData, DirectorySubspace runningShardKeys, Map<Integer, DirectorySubspace> shardMetrics, Map<Integer, DirectorySubspace> shardData) {
        this.topic = topic;
        this.assignments = assignments;
        this.heartbeats = heartbeats;
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

    public byte[] getTopicAssignmentsKey(Integer shardIndex) {
        return assignments.pack(shardIndex);
    }

    @Override
    public String toString() {
        return "{" +
                "'topic' : '" + topic + '\'' +
                '}';
    }
}
