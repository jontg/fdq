package com.relateiq.fdq;

import com.foundationdb.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mbessler on 2/6/15.
 */
public class Consumer {
    public static final Logger log = LoggerFactory.getLogger(Consumer.class);
    public static final int EXECUTOR_QUEUE_SIZE = 1000;
    public static final int HEARTBEAT_MILLIS = 2000;
    private final Database db;

    public Consumer(Database db) {
        this.db = db;
    }

    public TopicConsumer createConsumer(final String topic, String consumerName, java.util.function.Consumer<Envelope> consumer) {
        TopicConfig topicConfig = Helpers.createTopicConfig(db, topic);

        ConsumerConfig consumerConfig = new ConsumerConfig(topicConfig,
                consumerName,
                consumer,
                Helpers.createExecutor(),
                Helpers.createExecutor());

        return new TopicConsumer(db, consumerConfig);
    }

}