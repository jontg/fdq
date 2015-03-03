package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Created by mbessler on 2/6/15.
 */
public class Producer {
    public static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final Random random = new Random();

    private final Database db;

    public Producer(Database db) {
        this.db = db;
    }

    public TopicProducer createProducer(String topic){
        return db.run((Function<Transaction, TopicProducer>)tr -> new TopicProducer(db, Helpers.createTopicConfig(db, topic)));
    }

}
