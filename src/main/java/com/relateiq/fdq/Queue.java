package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.KeyValue;
import com.foundationdb.MutationType;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Created by mbessler on 2/6/15.
 */
public class Queue {
    public static final Logger log = LoggerFactory.getLogger(Queue.class);

    public static final Charset CHARSET = Charset.forName("UTF-8");
    public static final byte[] ONE = Helpers.intToByteArray(1);
    public static final int NUM_EXECUTORS = 10;
    public static final int MOD_HASH_ITERATIONS_QUEUE_SHARDING = 1;
    public static final int MOD_HASH_ITERATIONS_EXECUTOR_SHARDING = 2; // this is for the
    private final Database db;
    private final Random random = new Random();
    private final ConcurrentHashMap<List<String>, DirectorySubspace> directories = new ConcurrentHashMap<>();

    public static final int NUM_TOKENS = 24;  // todo: make this configurable

    public Queue(Database db) {
        this.db = db;
    }

    public void enqueue(final String topic, final String shardKey, final byte[] message) {
        enqueueBatch(topic, ImmutableList.of(new MessageRequest(shardKey, message)));
    }

    public void enqueueBatch(final String topic, final Collection<MessageRequest> messageRequests) {
        db.run((Function<Transaction, Void>) tr -> {
            for (MessageRequest messageRequest : messageRequests) {
                Integer shardIndex = Helpers.modHash(messageRequest.shardKey, NUM_TOKENS, MOD_HASH_ITERATIONS_QUEUE_SHARDING);

                DirectorySubspace dataDir = getCachedDirectory(Arrays.asList(topic, "data", "" + shardIndex));
                byte[] topicWatchKey = getTopicShardWatchKey(topic, shardIndex);

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

    private byte[] getTopicShardWatchKey(String topic, Integer shardIndex) {
        return getCachedDirectory(Arrays.asList(topic, "metrics", "" + shardIndex)).pack(Tuple.from("inserted"));
    }

    private DirectorySubspace getTopicAssignmentsDirectory(String topic) {
        return getCachedDirectory(Arrays.asList(topic, "config", "assignments"));
    }

    private DirectorySubspace getTopicDirectory(String topic) {
        return getCachedDirectory(Arrays.asList(topic));
    }

    private byte[] getTopicAssignmentsKey(String topic, Integer shardIndex) {
        return getTopicAssignmentsDirectory(topic).pack(shardIndex);
    }


    private DirectorySubspace getCachedDirectory(List<String> strings) {
        List<String> key = strings;
        DirectorySubspace result = directories.get(key);
        if (result != null) {
            return result;
        }
        result = db.run((Function<Transaction, DirectorySubspace>) tr -> DirectoryLayer.getDefault().createOrOpen(tr, key).get());
        directories.putIfAbsent(key, result);
        return result;
    }

    public Multimap<String, Integer> fetchAssignments(Transaction tr, String topic) {
        ImmutableMultimap.Builder<String, Integer> builder = ImmutableMultimap.builder();

        DirectorySubspace directory = getTopicAssignmentsDirectory(topic);
        for (KeyValue keyValue : tr.getRange(Range.startsWith(directory.pack()))) {
            Integer shardIndex = (int) directory.unpack(keyValue.getKey()).getLong(0);
            String consumerName = new String(keyValue.getValue(), CHARSET);
            builder.put(consumerName, shardIndex);
        }

        return builder.build();
    }

    public Optional<String> fetchAssignment(Transaction tr, String topic, Integer shardIndex) {
        byte[] bytes = tr.get(getTopicAssignmentsKey(topic, shardIndex)).get();
        if (bytes == null) {
            return Optional.empty();
        }
        return Optional.of(new String(bytes, CHARSET));
    }

    public void saveAssignments(Transaction tr, String topic, Multimap<String, Integer> assignments) {
        for (Map.Entry<String, Collection<Integer>> entry : assignments.asMap().entrySet()) {
            for (Integer integer : entry.getValue()) {
                tr.set(getTopicAssignmentsKey(topic, integer), entry.getKey().getBytes(CHARSET));
            }
        }
    }

    public void tail(final String topic, String consumerName, Consumer<Envelope> consumer) {
        // because we want to ensure we dont execute 2 tuples with same shard key at same time we want N different single-worker executors
        ImmutableMap.Builder<Integer, ExecutorService> executorsBuilder = ImmutableMap.builder();
        for (int i = 0; i < NUM_EXECUTORS; i++) {
            executorsBuilder.put(i, Executors.newFixedThreadPool(1));
        }
        ImmutableMap<Integer, ExecutorService> executors = executorsBuilder.build();

        Collection<Integer> tokens = addConsumer(topic, consumerName);

        // ensure tailers for each of our shards
        for (Integer token : tokens) {
            final int finalI = token;
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
            tr.clear(Range.startsWith(getTopicAssignmentsDirectory(topic).pack()));
            return null;
        });
    }

    public void nukeTopic(String topic) {

        db.run((Function<Transaction, Void>) tr -> {
            tr.clear(Range.startsWith(getTopicDirectory(topic).pack()));
            return null;
        });
    }


    private Collection<Integer> addConsumer(String topic, String name) {

        return db.run((Function<Transaction, Collection<Integer>>) tr -> {
            // fetch currentAssignments from fdb
            Multimap<String, Integer> assignments = fetchAssignments(tr, topic);

            // add this consumer
            assignments = Divvy.addConsumer(assignments, name, NUM_TOKENS);
            log.debug("Assignments updated: " + assignments);

            // save currentAssignments
            saveAssignments(tr, topic, assignments);
            return assignments.get(name);
        });


    }

    public void tailShard(final String topic, final Integer shardIndex, ImmutableMap<Integer, ExecutorService> executors, Consumer<Envelope> consumer, String consumerName) {
        DirectorySubspace dataDir = getCachedDirectory(Arrays.asList(topic, "data", "" + shardIndex));
        byte[] topicWatchKey = getTopicShardWatchKey(topic, shardIndex);

        while (tailWork(dataDir, topicWatchKey, executors, consumer, topic, consumerName, shardIndex)) {

        }
    }


    private boolean tailWork(DirectorySubspace dataDir, byte[] topicWatchKey, ImmutableMap<Integer, ExecutorService> executors, Consumer<Envelope> consumer, String topic, String consumerName, Integer shardIndex) {
        // ensure we still have this token, otherwise return false to stop tailing this token


        // casting is here because of a java 8 compiler bug with ambiguous overloads :(
        Optional<Future<Void>> watch = db.run((Function<Transaction, Optional<Future<Void>>>) tr -> {
            // try to read from queue first?
            if (!isMine(topic, consumerName, shardIndex, tr)) {
                return Optional.empty();
            }
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
                    int executorIndex = Helpers.modHash(shardKey, NUM_EXECUTORS, MOD_HASH_ITERATIONS_EXECUTOR_SHARDING);
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

    private void consumerWrapper(Consumer<Envelope> consumer, Envelope envelope){
        try {
            consumer.accept(envelope);
        }catch (Exception e){
            // todo: better handling of errors
            log.error("error during consume", e);
        }
    }

    private boolean isMine(String topic, String consumerName, Integer shardIndex, Transaction tr) {
        return fetchAssignment(tr, topic, shardIndex)
                .filter(n -> consumerName.equals(n)).isPresent();
    }

}
