package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.KeyValue;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.relateiq.fdq.DirectoryCache.directory;
import static com.relateiq.fdq.Helpers.getTopicAssignmentPath;
import static com.relateiq.fdq.Helpers.getTopicAssignmentsKey;
import static com.relateiq.fdq.Helpers.getTopicShardWatchKey;

/**
 * Created by mbessler on 2/6/15.
 */
public class Consumer {
    public static final Logger log = LoggerFactory.getLogger(Consumer.class);

    private final Database db;

    public Consumer(Database db) {
        this.db = db;
    }


    /**
     *
     * @param tr the transaction
     * @param topic the topic to fetch assignments for
     * @return the assignments, a multimap of shardIndexes indexed by consumerName
     */
    Multimap<String, Integer> fetchAssignments(Transaction tr, String topic) {
        ImmutableMultimap.Builder<String, Integer> builder = ImmutableMultimap.builder();

        DirectorySubspace directory = directory(tr, getTopicAssignmentPath(topic));
        for (KeyValue keyValue : tr.getRange(Range.startsWith(directory.pack()))) {
            Integer shardIndex = (int) directory.unpack(keyValue.getKey()).getLong(0);
            String consumerName = new String(keyValue.getValue(), Helpers.CHARSET);
            builder.put(consumerName, shardIndex);
        }

        return builder.build();
    }

    /**
     *
     * @param tr the transaction
     * @param topic the topic to fetch an assignment for
     * @param shardIndex the
     * @return
     */
    private Optional<String> fetchAssignment(Transaction tr, String topic, Integer shardIndex) {
        byte[] bytes = tr.get(getTopicAssignmentsKey(tr, topic, shardIndex)).get();
        if (bytes == null) {
            return Optional.empty();
        }
        return Optional.of(new String(bytes, Helpers.CHARSET));
    }

    void saveAssignments(Transaction tr, String topic, Multimap<String, Integer> assignments) {
        for (Map.Entry<String, Collection<Integer>> entry : assignments.asMap().entrySet()) {
            for (Integer integer : entry.getValue()) {
                tr.set(getTopicAssignmentsKey(tr, topic, integer), entry.getKey().getBytes(Helpers.CHARSET));
            }
        }
    }

    public void tail(final String topic, String consumerName, java.util.function.Consumer<Envelope> consumer) {
        // because we want to ensure we dont execute 2 tuples with same shard key at same time we want N different single-worker executors
        ImmutableMap.Builder<Integer, ExecutorService> executorsBuilder = ImmutableMap.builder();
        for (int i = 0; i < Helpers.NUM_EXECUTORS; i++) {
            executorsBuilder.put(i, Executors.newFixedThreadPool(1));
        }
        ImmutableMap<Integer, ExecutorService> executors = executorsBuilder.build();

        Collection<Integer> shards = addConsumer(topic, consumerName);

        // ensure tailers for each of our shards
        for (Integer shard : shards) {
            final int finalI = shard;
            new Thread(() -> {
                try {
                    tailShard(topic, finalI, executors, consumer, consumerName);
                } catch (Exception e){
                    log.error("error tailing", e);
                }
            }).start();
        }
    }

    public void clearAssignments(String topic) {
        db.run((Function<Transaction, Void>) tr -> {
            tr.clear(Range.startsWith(directory(tr, getTopicAssignmentPath(topic)).pack()));
            return null;
        });
    }

    /**
     * This will remove all configuration and data information about a topic. PERMANENTLY!
     *
     * BE CAREFUL
     *
     * @param topic
     */
    public void nukeTopic(String topic) {

        db.run((Function<Transaction, Void>) tr -> {
            tr.clear(Range.startsWith(directory(tr, topic).pack()));
            return null;
        });
    }


    private Collection<Integer> addConsumer(String topic, String name) {

        return db.run((Function<Transaction, Collection<Integer>>) tr -> {
            // fetch currentAssignments from fdb
            Multimap<String, Integer> assignments = fetchAssignments(tr, topic);

            // add this consumer
            assignments = Divvy.addConsumer(assignments, name, Helpers.NUM_SHARDS);
            log.debug("Assignments updated: " + assignments);

            // save currentAssignments
            saveAssignments(tr, topic, assignments);
            return assignments.get(name);
        });

    }

    private Collection<Integer> removeConsumer(String topic, String name) {

        return db.run((Function<Transaction, Collection<Integer>>) tr -> {
            // fetch currentAssignments from fdb
            Multimap<String, Integer> assignments = fetchAssignments(tr, topic);

            // add this consumer
            assignments = Divvy.addConsumer(assignments, name, Helpers.NUM_SHARDS);
            log.debug("Assignments updated: " + assignments);

            // save currentAssignments
            saveAssignments(tr, topic, assignments);
            return assignments.get(name);
        });

    }

    private void tailShard(final String topic, final Integer shardIndex, ImmutableMap<Integer, ExecutorService> executors, java.util.function.Consumer<Envelope> consumer, String consumerName) {
        while (tailWork( executors, consumer, topic, consumerName, shardIndex)) {

        }
    }


    private boolean tailWork(ImmutableMap<Integer, ExecutorService> executors, java.util.function.Consumer<Envelope> consumer, String topic, String consumerName, Integer shardIndex) {
        // ensure we still have this shard, otherwise return false to stop tailing this shard


        // casting is here because of a java 8 compiler bug with ambiguous overloads :(
        Optional<Future<Void>> watch = db.run((Function<Transaction, Optional<Future<Void>>>) tr -> {
            // try to read from queue first?
            if (!isMine(topic, consumerName, shardIndex, tr)) {
                return Optional.empty();
            }
            byte[] topicWatchKey = getTopicShardWatchKey(tr, topic, shardIndex);
            return Optional.of(tr.watch(topicWatchKey));
        });

        if (!watch.isPresent()) {
            log.debug("Assignment changed, shutting down tail topic=" + topic + " consumeName=" + consumerName + " shardIndex=" + shardIndex);
            return false;
        }
//        watch.map((Function<Void, Void>) aVoid -> {
//            return null;
//        });

        watch.get();

        // casting is here because of a java 8 compiler bug with ambiguous overloads :(
        List<Envelope> fetched = db.run((Function<Transaction, List<Envelope>>) tr -> {
            // make sure we dont read things that are no longer ours
            if (!isMine(topic, consumerName, shardIndex, tr)) {
                return ImmutableList.of();
            }

            DirectorySubspace dataDir = directory(tr, Helpers.getShardDataPath(topic, shardIndex));

            List<Envelope> result = new ArrayList<>();
            for (KeyValue keyValue : tr.snapshot().getRange(Range.startsWith(dataDir.pack()))) {
                tr.addReadConflictKey(keyValue.getKey());
                tr.clear(keyValue.getKey());

                try {

                    Tuple key = dataDir.unpack(keyValue.getKey());
                    Tuple value = Tuple.fromBytes(keyValue.getValue());
//                log.debug("kv: " + key.getLong(2) + " " + key.getLong(3) + " : " + new String(keyValue.getValue()));

                    long insertionTime = key.getLong(0);

                    String shardKey = value.getString(0);
                    byte[] message = value.getBytes(1);
                    int executorIndex = Helpers.modHash(shardKey, Helpers.NUM_EXECUTORS, Helpers.MOD_HASH_ITERATIONS_EXECUTOR_SHARDING);
                    result.add(new Envelope(insertionTime, shardKey, shardIndex, executorIndex, message));
                } catch (Exception e){
                    // issue with deserialization
                    log.warn("error popping", e);
                }
            }

            return result;
        });

        for (Envelope envelope : fetched) {

            if (log.isTraceEnabled()) {
                log.trace("Submitting to executor topic=" + topic + " " + envelope.toString());
            }
            executors.get(envelope.executorIndex).submit(() -> consumerWrapper(consumer, envelope));
        }

        return true;
    }

    private void consumerWrapper(java.util.function.Consumer<Envelope> consumer, Envelope envelope){
        try {
            consumer.accept(envelope);
        }catch (Exception e){
            // todo: better handling of errors
            log.error("error during consume", e);
        }
    }

    private boolean isMine(String topic, String consumerName, Integer shardIndex, Transaction tr) {
        return fetchAssignment(tr, topic, shardIndex)
                .filter(consumerName::equals).isPresent();
    }

}
