package com.relateiq.fdq;

import com.foundationdb.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mbessler on 2/6/15.
 */
public class Consumer {
    public static final Logger log = LoggerFactory.getLogger(Consumer.class);
    private final Database db;

    public Consumer(Database db) {
        this.db = db;
    }

    public TopicConsumer createConsumer(final String topic, String consumerName, java.util.function.Consumer<Envelope> consumer) {
        TopicConfig topicConfig = Helpers.createTopicConfig(db, topic);

        return new TopicConsumer(db, topicConfig, consumerName, consumer);
    }

}