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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.relateiq.fdq.DirectoryCache.mkdirp;
import static com.relateiq.fdq.DirectoryCache.rmdir;
import static com.relateiq.fdq.Helpers.NUM_SHARDS;
import static com.relateiq.fdq.Helpers.bytesToMillis;
import static com.relateiq.fdq.Helpers.currentTimeMillisAsBytes;
import static com.relateiq.fdq.Helpers.getTopicAssignmentPath;
import static com.relateiq.fdq.Helpers.getTopicHeartbeatPath;

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
     * @param topicAssignmentDirectory
     * @return the assignments, a multimap of shardIndexes indexed by consumerName
     */
    Multimap<String, Integer> fetchAssignments(Transaction tr, DirectorySubspace topicAssignmentDirectory) {
        ImmutableMultimap.Builder<String, Integer> builder = ImmutableMultimap.builder();

        for (KeyValue keyValue : tr.getRange(Range.startsWith(topicAssignmentDirectory.pack()))) {
            Integer shardIndex = (int) topicAssignmentDirectory.unpack(keyValue.getKey()).getLong(0);
            String consumerName = new String(keyValue.getValue(), Helpers.CHARSET);
            builder.put(consumerName, shardIndex);
        }

        return builder.build();
    }

    /**
     *
     * @param tr the transaction
     * @param directories
     * @param shardIndex the
     * @return
     */
    private Optional<String> fetchAssignment(Transaction tr, TopicDirectories directories, Integer shardIndex) {
        byte[] bytes = tr.get(directories.getTopicAssignmentsKey(shardIndex)).get();
        if (bytes == null) {
            return Optional.empty();
        }
        return Optional.of(new String(bytes, Helpers.CHARSET));
    }

    void saveAssignments(Transaction tr, TopicDirectories directories, Multimap<String, Integer> assignments) {
        for (Map.Entry<String, Collection<Integer>> entry : assignments.asMap().entrySet()) {
            for (Integer integer : entry.getValue()) {
                tr.set(directories.getTopicAssignmentsKey(integer), entry.getKey().getBytes(Helpers.CHARSET));
            }
        }
    }

    public void consume(final String topic, String consumerName, java.util.function.Consumer<Envelope> consumer) {
        ImmutableMap<Integer, ExecutorService> executors = getExecutors(Helpers.NUM_EXECUTORS);

        TopicDirectories directories = ensureDirectories(topic);

        Collection<Integer> shards = addConsumer(topic, consumerName, directories);

        // start heartbeat thread
        new Thread(() -> {
            while (true) {
//                heartbeat(topic, consumerName);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.info("heartbeat interrupted, closing consume topic=" + topic + " consumerName=" + consumerName);
                    break;
                }
            }
        });

        // ensure consumeers for each of our shards
        for (Integer shard : shards) {
            final int finalI = shard;
            new Thread(() -> {
                try {
                    consumeShard(topic, finalI, executors, consumer, consumerName, directories);
                } catch (Exception e){
                    log.error("error consuming", e);
                }
                log.debug("Shutting down consumer shard thread topic=" + topic + " consumeName=" + consumerName + " shardIndex=" + shard);
            }).start();
        }
    }


    private TopicDirectories ensureDirectories(String topic) {
        return db.run((Function<Transaction, TopicDirectories>) tr -> {
            DirectorySubspace assignments = mkdirp(tr, getTopicAssignmentPath(topic));
            DirectorySubspace heartbeats = mkdirp(tr, getTopicHeartbeatPath(topic));

            ImmutableMap.Builder<Integer, DirectorySubspace> shardMetrics = ImmutableMap.builder();
            ImmutableMap.Builder<Integer, DirectorySubspace> shardData = ImmutableMap.builder();
            for (int i = 0; i < NUM_SHARDS; i++) {
                shardMetrics.put(i, mkdirp(tr, Helpers.getTopicShardMetricPath(topic, i)));
                shardData.put(i, mkdirp(tr, Helpers.getTopicShardDataPath(topic, i)));
            }

            return new TopicDirectories(assignments, heartbeats, shardMetrics.build(), shardData.build());
        });
    }

    private ImmutableMap<Integer, ExecutorService> getExecutors(int numExecutors) {
        // because we want to ensure we dont execute 2 tuples with same shard key at same time we want N different single-worker executors
        ImmutableMap.Builder<Integer, ExecutorService> executorsBuilder = ImmutableMap.builder();
        for (int i = 0; i < numExecutors; i++) {
            executorsBuilder.put(i, Executors.newFixedThreadPool(1));
        }
        return executorsBuilder.build();
    }

    private void heartbeat(String topic, String consumerName, TopicDirectories directories) {
        db.run((Function<Transaction, Void>) tr -> {
            // look for dead consumers
            DirectorySubspace directory = directories.heartbeats;
            long now = System.currentTimeMillis();
            for (KeyValue keyValue : tr.snapshot().getRange(Range.startsWith(directory.pack()))) {
                String name = directory.unpack(keyValue.getValue()).getString(0);
                long millis = bytesToMillis(keyValue.getValue());

                if (now - millis > 5000) {
                    removeConsumer(tr, topic, name, directories);
                }
            }
            tr.set(directory.pack(consumerName), currentTimeMillisAsBytes());
            return null;
        });
    }

    public void clearAssignments(String topic) {
        db.run((Function<Transaction, Void>) tr -> {
            rmdir(tr, getTopicAssignmentPath(topic));
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
            rmdir(tr, topic);
            return null;
        });
    }


    private Collection<Integer> addConsumer(String topic, String name, TopicDirectories directories) {

        return db.run((Function<Transaction, Collection<Integer>>) tr -> {
            // fetch currentAssignments from fdb
            Multimap<String, Integer> assignments = fetchAssignments(tr, directories.assignments);
            log.debug("Assignments updating, currently name=" + name + " assignments=" + assignments);

            // add this consumer
            assignments = Divvy.addConsumer(assignments, name, Helpers.NUM_SHARDS);
            log.debug("Assignments updated, added name=" + name + " assignments=" + assignments);

            // save currentAssignments
            saveAssignments(tr, directories, assignments);
            return assignments.get(name);
        });

    }

    private Collection<Integer> removeConsumer(String topic, String name, TopicDirectories directories) {

        return db.run((Function<Transaction, Collection<Integer>>) tr -> removeConsumer(tr, topic, name, directories));

    }

    private Collection<Integer> removeConsumer(Transaction tr, String topic, String name, TopicDirectories directories) {
        // fetch currentAssignments from fdb
        Multimap<String, Integer> assignments = fetchAssignments(tr, directories.assignments);

        // add this consumer
        assignments = Divvy.removeConsumer(assignments, name, Helpers.NUM_SHARDS);
        log.debug("Assignments updated, removed name=" + name + " assignments=" + assignments);

        // save currentAssignments
        saveAssignments(tr, directories, assignments);
        return assignments.get(name);
    }

    private void consumeShard(final String topic, final Integer shardIndex, ImmutableMap<Integer, ExecutorService> executors, java.util.function.Consumer<Envelope> consumer, String consumerName, TopicDirectories directories) {
        while (consumeWork(executors, consumer, topic, consumerName, shardIndex, directories)) {

        }
    }


    private boolean consumeWork(ImmutableMap<Integer, ExecutorService> executors, java.util.function.Consumer<Envelope> consumer, String topic, String consumerName, Integer shardIndex, TopicDirectories directories) {
        // ensure we still have this shard, otherwise return false to stop consuming this shard


        // casting is here because of a java 8 compiler bug with ambiguous overloads :(
        Optional<Future<Void>> watch = db.run((Function<Transaction, Optional<Future<Void>>>) tr -> {
            // try to read from queue first?
            if (!isMine(tr, directories, shardIndex, consumerName)) {
                return Optional.empty();
            }
            byte[] topicWatchKey = directories.getTopicShardWatchKey(shardIndex);
            return Optional.of(tr.watch(topicWatchKey));
        });

        if (!watch.isPresent()) {
            log.debug("Assignment changed, shutting down consume topic=" + topic + " consumeName=" + consumerName + " shardIndex=" + shardIndex);
            return false;
        }
//        watch.map((Function<Void, Void>) aVoid -> {
//            return null;
//        });

        watch.get();

        // casting is here because of a java 8 compiler bug with ambiguous overloads :(
        List<Envelope> fetched = db.run((Function<Transaction, List<Envelope>>) tr -> {
            // make sure we dont read things that are no longer ours
            if (!isMine(tr, directories, shardIndex, consumerName)) {
                return ImmutableList.of();
            }

            DirectorySubspace dataDir = directories.shardData.get(shardIndex);

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

    private boolean isMine(Transaction tr, TopicDirectories directories, Integer shardIndex, String consumerName) {
        return fetchAssignment(tr, directories, shardIndex)
                .filter(consumerName::equals).isPresent();
    }

}
