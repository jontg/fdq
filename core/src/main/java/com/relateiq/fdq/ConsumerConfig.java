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

    public static final int DEFAULT_NUM_EXECUTORS = 10;


    public final TopicConfig topicConfig;
    public final java.util.function.Consumer<Envelope> consumer;
    public final String name;

    public final Map<Integer, ExecutorService> executors;
    public Map<Integer, Thread> shardThreads = Maps.newHashMap();

    public final long sleepMillisBetweenBatches = 0;
    public final int batchSize = 10;
    public final int numExecutors = DEFAULT_NUM_EXECUTORS;


    public ConsumerConfig(TopicConfig topicConfig, String name, Consumer<Envelope> consumer, ImmutableMap<Integer, ExecutorService> executors) {
        this.topicConfig = topicConfig;
        this.name = name;
        this.consumer = consumer;
        this.executors = executors;
    }


    @Override
    public String toString() {
        return "{" +
                "'topic' : '" + topicConfig.topic + '\'' +
                ", 'name' : '" + name + '\'' +
                '}';
    }
}
