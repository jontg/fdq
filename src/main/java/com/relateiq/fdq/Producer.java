package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.MutationType;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Random;

import static com.relateiq.fdq.DirectoryCache.directory;
import static com.relateiq.fdq.Helpers.*;

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

    /**
     * Enqueue messages into a topic
     *
     * @param topic the unique identifier for the topic
     * @param shardKey the key used to consistently route this message to the same executor/thread (via the same consumer)
     * @param message the actual message
     */
    public void enqueue(final String topic, final String shardKey, final byte[] message) {
        enqueueBatch(topic, ImmutableList.of(new MessageRequest(shardKey, message)));
    }

    /**
     * Enqueue multiple messages into a topic. Much faster than adding them individually.
     *
     * @param topic the unique identifier for the topic
     * @param messageRequests the shardKey/message pairs to add to this topic in batch
     */
    public void enqueueBatch(final String topic, final Collection<MessageRequest> messageRequests) {
        db.run((Function<Transaction, Void>) tr -> {
            for (MessageRequest messageRequest : messageRequests) {
                Integer shardIndex = modHash(messageRequest.shardKey, NUM_SHARDS, MOD_HASH_ITERATIONS_QUEUE_SHARDING);

                DirectorySubspace dataDir = directory(tr, getTopicShardDataPath(topic, shardIndex));
                byte[] topicWatchKey = getTopicShardWatchKey(tr, topic, shardIndex);

                // TODO: ensure monotonic
                if (log.isTraceEnabled()) {
                    log.trace("enqueuing topic=" + topic + " shardKey=" + messageRequest.shardKey + " shardIndex=" + shardIndex);
                }

                tr.set(dataDir.pack(Tuple.from(System.currentTimeMillis(), random.nextInt())), Tuple.from(messageRequest.shardKey, messageRequest.message).pack());
                tr.mutate(MutationType.ADD, topicWatchKey, ONE);
            }
            return null;
        });
    }


}
