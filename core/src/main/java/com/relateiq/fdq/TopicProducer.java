package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.MutationType;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Random;

import static com.relateiq.fdq.Helpers.MOD_HASH_ITERATIONS_QUEUE_SHARDING;
import static com.relateiq.fdq.Helpers.ONE;
import static com.relateiq.fdq.Helpers.modHash;

/**
 * Created by mbessler on 2/6/15.
 */
public class TopicProducer {
    public static final Logger log = LoggerFactory.getLogger(TopicProducer.class);

    private final Random random = new Random();

    private final Database db;
    private final TopicConfig topicConfig;

    public TopicProducer(Database db, TopicConfig topicConfig) {
        this.db = db;
        this.topicConfig = topicConfig;
    }

    /**
     * Enqueue messages into a topic
     * @param shardKey the key used to consistently route this message to the same executor/thread (via the same consumer)
     * @param message the actual message
     */
    public void produce(final String shardKey, final byte[] message) {
        produceBatch(ImmutableList.of(new MessageRequest(shardKey, message)));
    }

    /**
     * Enqueue multiple messages into a topic. Much faster than adding them individually.
     *
     * @param messageRequests the shardKey/message pairs to add to this topic in batch
     */
    public void produceBatch(final Collection<MessageRequest> messageRequests) {
        db.run((Function<Transaction, Void>) tr -> {
            for (MessageRequest messageRequest : messageRequests) {
                Integer shardIndex = modHash(messageRequest.shardKey, topicConfig.numShards, MOD_HASH_ITERATIONS_QUEUE_SHARDING);

                if (log.isTraceEnabled()) {
                    log.trace("producing topic=" + topicConfig.topic + " shardKey=" + messageRequest.shardKey + " shardIndex=" + shardIndex);
                }

                // TODO: ensure monotonic
                tr.set(topicConfig.shardData.get(shardIndex).pack(Tuple.from(System.currentTimeMillis(), random.nextInt())), Tuple.from(messageRequest.shardKey, messageRequest.message).pack());
                tr.mutate(MutationType.ADD, topicConfig.shardMetricInserted(shardIndex), ONE);
            }
            return null;
        });
    }


}
