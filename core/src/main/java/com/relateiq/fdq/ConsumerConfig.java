package com.relateiq.fdq;

import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Created by mbessler on 2/23/15.
 */
public class ConsumerConfig {

    public final java.util.function.Consumer<Envelope> consumer;
    public final String topic;
    public final String name;

    public final DirectorySubspace assignments;
    public final DirectorySubspace heartbeats;
    public final Map<Integer, DirectorySubspace> shardMetrics;
    public final Map<Integer, DirectorySubspace> shardData;
    public final Map<Integer, ExecutorService> executors;
    public Map<Integer, Thread> shardThreads = Maps.newHashMap();


    public ConsumerConfig(String topic, String name, Consumer<Envelope> consumer, DirectorySubspace assignments, DirectorySubspace heartbeats, Map<Integer, DirectorySubspace> shardMetrics, Map<Integer, DirectorySubspace> shardData, ImmutableMap<Integer, ExecutorService> executors) {
        this.topic = topic;
        this.name = name;
        this.consumer = consumer;
        this.assignments = assignments;
        this.heartbeats = heartbeats;
        this.shardMetrics = shardMetrics;
        this.shardData = shardData;
        this.executors = executors;
    }

    public byte[] getTopicShardWatchKey(Integer shardIndex) {
        return shardMetrics.get(shardIndex).pack(Tuple.from("inserted"));
    }

    public byte[] getTopicAssignmentsKey(Integer shardIndex) {
        return assignments.pack(shardIndex);
    }

    @Override
    public String toString() {
        return "{" +
                "'topic' : '" + topic + '\'' +
                ", 'name' : '" + name + '\'' +
                '}';
    }
}
