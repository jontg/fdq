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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    public static final int CONSUMER_TIMEOUT_MILLIS = 5000;
    public static final int BATCH_SIZE = 10;
    public static final int EXECUTOR_QUEUE_SIZE = 1000;
    private static final byte[] NULL = {0};
    private final Database db;

    public Consumer(Database db) {
        this.db = db;
    }

    private static void consumerWrapper(java.util.function.Consumer<Envelope> consumer, Envelope envelope) {
        try {
            consumer.accept(envelope);
        } catch (Exception e) {
            // todo: better handling of errors
            log.error("error during consume", e);
        }
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
    private Optional<String> fetchAssignment(Transaction tr, ConsumerConfig directories, Integer shardIndex) {
        byte[] bytes = tr.get(directories.getTopicAssignmentsKey(shardIndex)).get();
        if (bytes == null) {
            return Optional.empty();
        }
        return Optional.of(new String(bytes, Helpers.CHARSET));
    }

    void saveAssignments(Transaction tr, ConsumerConfig directories, Multimap<String, Integer> assignments) {
        for (Map.Entry<String, Collection<Integer>> entry : assignments.asMap().entrySet()) {
            for (Integer integer : entry.getValue()) {
                tr.set(directories.getTopicAssignmentsKey(integer), entry.getKey().getBytes(Helpers.CHARSET));
            }
        }
    }

    public void consume(final String topic, String consumerName, java.util.function.Consumer<Envelope> consumer) {
        ConsumerConfig consumerConfig = initConsumer(topic, consumerName, consumer);

        // start heartbeat thread
        new Thread(() -> {
            while (true) {
                if (log.isTraceEnabled()) {
                    log.trace("heartbeat " + consumerConfig.toString());
                }
                heartbeat(consumerConfig);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    log.info("heartbeat interrupted, closing consume topic=" + topic + " consumerName=" + consumerName);
                    break;
                }
            }
        }).start();

    }

    private void ensureShardThreads(ConsumerConfig consumerConfig, Collection<Integer> shards) {
        // ensure consumers for each of our shards
        for (Integer shard : shards) {
            Thread thread = consumerConfig.shardThreads.get(shard);
            if (thread != null && thread.isAlive()) {
                continue;
            }

            final int finalI = shard;

            log.info("Starting thread for " + consumerConfig.toString() + " shard="  + shard);
            thread = new Thread(() -> {
                try {
                    consumeShard(consumerConfig, finalI);
                } catch (Exception e) {
                    log.error("error consuming", e);
                }
                consumerConfig.shardThreads.remove(shard);
                log.info("Shutting down consumer shard thread topic=" + consumerConfig.topic + " consumeName=" + consumerConfig.name + " shardIndex=" + shard);
            });
//            consumeShardAsync(consumerConfig, finalI);

            consumerConfig.shardThreads.put(shard, thread);
            thread.start();
            // add thread shutdown hook to remove it 
        }
    }

    /**
     *
     * @param topic
     * @param consumerName
     * @param consumer
     * @return ConsumerConfig
     */
    private ConsumerConfig initConsumer(String topic, String consumerName, java.util.function.Consumer<Envelope> consumer) {
        return db.run((Function<Transaction, ConsumerConfig>) tr -> {
            // init directories
            DirectorySubspace assignments = mkdirp(tr, getTopicAssignmentPath(topic));
            DirectorySubspace heartbeats = mkdirp(tr, getTopicHeartbeatPath(topic));

            ImmutableMap.Builder<Integer, DirectorySubspace> shardMetrics = ImmutableMap.builder();
            ImmutableMap.Builder<Integer, DirectorySubspace> shardData = ImmutableMap.builder();
            for (int i = 0; i < NUM_SHARDS; i++) {
                shardMetrics.put(i, mkdirp(tr, Helpers.getTopicShardMetricPath(topic, i)));
                shardData.put(i, mkdirp(tr, Helpers.getTopicShardDataPath(topic, i)));
            }

            // register consumer
            tr.set(heartbeats.pack(consumerName), currentTimeMillisAsBytes());

            // TODO: configurable # of executors
            ImmutableMap<Integer, ExecutorService> executors = getExecutors(ConsumerConfig.NUM_EXECUTORS);

            return new ConsumerConfig(topic, consumerName, consumer, assignments, heartbeats, shardMetrics.build(), shardData.build(), executors);
        });
    }

    private ImmutableMap<Integer, ExecutorService> getExecutors(int numExecutors) {
        // because we want to ensure we dont execute 2 tuples with same shard key at same time we want N different single-worker executors
        ImmutableMap.Builder<Integer, ExecutorService> executorsBuilder = ImmutableMap.builder();
        for (int i = 0; i < numExecutors; i++) {
            executorsBuilder.put(i, new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(EXECUTOR_QUEUE_SIZE)));
        }
        return executorsBuilder.build();
    }

    private void heartbeat(ConsumerConfig consumerConfig) {

        HeartbeatResult result = db.run((Function<Transaction, HeartbeatResult>) tr -> heartbeat(tr, consumerConfig));

        ensureShardThreads(consumerConfig, result.assignments.get(consumerConfig.name));

        if (result.isAssignmentsUpdated) {
            log.debug("Assignments updated, topic=" + consumerConfig.topic + " removedConsumers=" + result.removedConsumers + " assignments=" + result.assignments);
        }

    }

    /**
     * This method is responsible for maintaing proper state
     *
     * This will:
     *  1. clean out dead consumers
     *  2. ensure this consumer is registered
     *
     * @param tr
     * @param consumerConfig
     * @return
     */
    private HeartbeatResult heartbeat(Transaction tr, ConsumerConfig consumerConfig) {
        ImmutableList.Builder<String> removedConsumersB = ImmutableList.builder();

        // look for dead consumers
        long now = System.currentTimeMillis();

        ImmutableSet.Builder<String> liveConsumersB = ImmutableSet.builder();
        for (KeyValue keyValue : tr.snapshot().getRange(Range.startsWith(consumerConfig.heartbeats.pack()))) {
            String consumerName = consumerConfig.heartbeats.unpack(keyValue.getKey()).getString(0);
            if (consumerName.equals(consumerConfig.name)){
                liveConsumersB.add(consumerConfig.name);
                continue;
            }
            long millis = bytesToMillis(keyValue.getValue());

            if (now - millis > CONSUMER_TIMEOUT_MILLIS) {
                removedConsumersB.add(consumerName);
            } else {
                liveConsumersB.add(consumerName);
            }
        }

        ImmutableSet<String> liveConsumers = liveConsumersB.build();
        Multimap<String, Integer> originalAssignments = fetchAssignments(tr, consumerConfig.assignments);

        originalAssignments.keySet().forEach(consumerName -> {
            if (!liveConsumers.contains(consumerName)){
                removedConsumersB.add(consumerName);
            }
        });

        ImmutableList<String> removedConsumers = removedConsumersB.build();
        Multimap<String, Integer> newAssignments = removeConsumers(tr, removedConsumers, consumerConfig, originalAssignments);

        // ensure our consumer is added
        newAssignments = Divvy.addConsumer(newAssignments, consumerConfig.name, Helpers.NUM_SHARDS);

        boolean isAssignmentsUpdated = !newAssignments.equals(originalAssignments);
        if (isAssignmentsUpdated) {
            saveAssignments(tr, consumerConfig, newAssignments);
        }

        // update our timestamp
        tr.set(consumerConfig.heartbeats.pack(consumerConfig.name), currentTimeMillisAsBytes());

        // check for de/activation


        return new HeartbeatResult(removedConsumers, newAssignments, isAssignmentsUpdated);
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


//    private Collection<Integer> removeConsumer(String topic, String name, TopicDirectories directories) {
//
//        return db.run((Function<Transaction, Collection<Integer>>) tr -> removeConsumer(tr, name, directories));
//
//    }

    private Multimap<String, Integer> removeConsumers(Transaction tr, Collection<String> names, ConsumerConfig consumerConfig, Multimap<String, Integer> assignments) {
        // fetch currentAssignments from fdb

        // remove these consumers
        for (String name : names) {
            assignments = Divvy.removeConsumer(assignments, name, Helpers.NUM_SHARDS);

            // unregister consumer / remove heartbeat
            tr.clear(consumerConfig.heartbeats.pack(name));
        }

        return assignments;
    }

    private void consumeShard(ConsumerConfig consumerConfig, final Integer shardIndex) {
        while (consumeWork(consumerConfig, shardIndex)) {}
    }

    private boolean consumeWork(ConsumerConfig consumerConfig, Integer shardIndex) {
        // casting is here because of a java 8 compiler bug with ambiguous overloads :(
        ConsumeWorkResult workResult = db.run((Function<Transaction, ConsumeWorkResult>) tr -> {
            // make sure we dont read things that are no longer ours
            if (!isMine(tr, consumerConfig, shardIndex)) {
                return new ConsumeWorkResult(null, null, false);
            }

            // FETCH SOME TUPLES
            DirectorySubspace dataDir = consumerConfig.shardData.get(shardIndex);
            List<Envelope> result = new ArrayList<>();
            int numFound = 0;
            for (KeyValue keyValue : tr.snapshot().getRange(Range.startsWith(dataDir.pack()), consumerConfig.batchSize)) {
                numFound++;
                tr.addReadConflictKey(keyValue.getKey());
                tr.clear(keyValue.getKey());

                try {

                    Tuple key = dataDir.unpack(keyValue.getKey());
                    Tuple value = Tuple.fromBytes(keyValue.getValue());
//                log.debug("kv: " + key.getLong(2) + " " + key.getLong(3) + " : " + new String(keyValue.getValue()));
                    long insertionTime = key.getLong(0);

                    String shardKey = value.getString(0);
                    byte[] message = value.getBytes(1);
                    int executorIndex = Helpers.modHash(shardKey, consumerConfig.numExecutors, Helpers.MOD_HASH_ITERATIONS_EXECUTOR_SHARDING);
                    result.add(new Envelope(insertionTime, shardKey, shardIndex, executorIndex, message));
                } catch (Exception e){
                    // issue with deserialization
                    log.warn("swallowed error during consume, lost message", e);
                }
            }

            if (numFound < BATCH_SIZE) {
                // exhausted
                byte[] topicWatchKey = consumerConfig.getTopicShardWatchKey(shardIndex);
                return new ConsumeWorkResult(tr.watch(topicWatchKey), result, true);
            }


            return new ConsumeWorkResult(null, result, true);
        });

        if (!workResult.isStillActive) {
            // shutting down!
            return false;
        }

        for (Envelope envelope : workResult.fetched) {
            if (log.isTraceEnabled()) {
                log.trace("Submitting to executor " + consumerConfig.toString() + " " + envelope.toString());
            }
            consumerConfig.executors.get(envelope.executorIndex).submit(() -> consumerWrapper(consumerConfig.consumer, envelope));
        }

        if (workResult.watch != null) {
            log.debug("Exhausted queue, watching, executor topic=" + consumerConfig.toString() + " shardIndex=" + shardIndex);
            workResult.watch.get();
        } else {
            //optionally sleep
            if (consumerConfig.sleepBetweenBatches != 0) {
                try {
                    Thread.sleep(consumerConfig.sleepBetweenBatches);
                } catch (InterruptedException e) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean isMine(Transaction tr, ConsumerConfig consumerConfig, Integer shardIndex) {
        return fetchAssignment(tr, consumerConfig, shardIndex)
                .filter(consumerConfig.name::equals).isPresent();
    }

    private static class ConsumeWorkResult {
        public final Future<Void> watch;
        public final List<Envelope> fetched;
        public final boolean isStillActive;

        private ConsumeWorkResult(Future<Void> watch, List<Envelope> fetched, boolean isStillActive) {
            this.watch = watch;
            this.fetched = fetched;
            this.isStillActive = isStillActive;
        }
    }

    private static class HeartbeatResult {
        public final List<String> removedConsumers;
        public final Multimap<String, Integer> assignments;
        public final boolean isAssignmentsUpdated;

        private HeartbeatResult(List<String> removedConsumers, Multimap<String, Integer> assignments, boolean isAssignmentsUpdated) {
            this.removedConsumers = removedConsumers;
            this.assignments = assignments;
            this.isAssignmentsUpdated = isAssignmentsUpdated;
        }
    }

}
