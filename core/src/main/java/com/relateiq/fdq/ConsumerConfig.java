package com.relateiq.fdq;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Created by mbessler on 2/23/15.
 */
public class ConsumerConfig {

    public static final int DEFAULT_NUM_EXECUTORS = 50;


    public final TopicConfig topicConfig;
    public final java.util.function.Consumer<Envelope> consumer;
    public final String name;

    public final ExecutorService executorOuter;
    public final ExecutorService executorInner;
    public Map<Integer, Thread> shardThreads = Maps.newHashMap();

    public final int numExecutors = DEFAULT_NUM_EXECUTORS;

    public long sleepMillisBetweenBatches = 0;
    public int batchSize = 100;
    public long sleepMillisOnAllSkipped = 1000;


    public ConsumerConfig(TopicConfig topicConfig, String name, Consumer<Envelope> consumer, ExecutorService executorOuter, ExecutorService executorInner) {
        this.topicConfig = topicConfig;
        this.name = name;
        this.consumer = consumer;
        this.executorOuter = executorOuter;
        this.executorInner = executorInner;
    }


    @Override
    public String toString() {
        return "{" +
                "'topic' : '" + topicConfig.topic + '\'' +
                ", 'name' : '" + name + '\'' +
                '}';
    }
}
