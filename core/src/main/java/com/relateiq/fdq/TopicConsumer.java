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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static com.relateiq.fdq.Helpers.bytesToMillis;
import static com.relateiq.fdq.Helpers.currentTimeMillisAsBytes;
import static com.relateiq.fdq.TopicConfig.METRIC_ACKED;
import static com.relateiq.fdq.TopicConfig.METRIC_ACKED_DURATION;
import static com.relateiq.fdq.TopicConfig.METRIC_ERRORED;
import static com.relateiq.fdq.TopicConfig.METRIC_ERRORED_DURATION;
import static com.relateiq.fdq.TopicConfig.METRIC_POPPED;
import static com.relateiq.fdq.TopicConfig.METRIC_SKIPPED;
import static com.relateiq.fdq.TopicConfig.METRIC_TIMED_OUT;

/**
 * Created by mbessler on 2/6/15.
 */
public class TopicConsumer {
    private static final Logger log = LoggerFactory.getLogger(TopicConsumer.class);

    public static final int EXECUTOR_QUEUE_SIZE = 1000;
    public static final int DEFAULT_NUM_EXECUTORS = 50;
    public static final int HEARTBEAT_MILLIS = 2000;

    private final Database db;
    private final TopicConfig topicConfig;
    private final String name;
    private final java.util.function.Consumer<Envelope> consumer;
    private final ExecutorService executorOuter;
    private final ExecutorService executorInner;
    private final int numExecutors = DEFAULT_NUM_EXECUTORS;

    private Map<Integer, Thread> shardThreads = Maps.newHashMap();

    private long sleepMillisBetweenBatches = 0;
    private int batchSize = 100;
    private long sleepMillisOnAllSkipped = 1000;

    public TopicConsumer(Database db, TopicConfig topicConfig, String consumerName, Consumer<Envelope> consumer) {
        this.db = db;
        this.executorInner = Helpers.createExecutor();
        this.executorOuter = Helpers.createExecutor();
        this.topicConfig = topicConfig;
        this.name = consumerName;
        this.consumer = consumer;

        // start heartbeat thread
        new Thread(() -> {
            while (true) {
                if (log.isTraceEnabled()) {
                    log.trace("heartbeat " + toString());
                }
                heartbeat();
                try {
                    Thread.sleep(HEARTBEAT_MILLIS);
                } catch (InterruptedException e) {
                    log.info("heartbeat interrupted, closing consumerConfig=" + toString());
                    break;
                }
            }
        }).start();

    }

    /**
     * Fetches the consumer name for a given shardIndex
     *
     * @param tr the transaction
     * @param shardIndex the
     * @return
     */
    private Optional<String> fetchAssignment(Transaction tr, Integer shardIndex) {
        byte[] bytes = tr.get(topicConfig.shardAssignmentKey(shardIndex)).get();
        if (bytes == null) {
            return Optional.empty();
        }
        return Optional.of(new String(bytes, Helpers.CHARSET));
    }

    private void saveAssignments(Transaction tr, Multimap<String, Integer> assignments) {
        for (Map.Entry<String, Collection<Integer>> entry : assignments.asMap().entrySet()) {
            for (Integer integer : entry.getValue()) {
                tr.set(topicConfig.shardAssignmentKey(integer), entry.getKey().getBytes(Helpers.CHARSET));
            }
        }
    }


    /**
     * Ensure we have a consumer thread for each of our shards
     *
     * @param shards
     */
    private void ensureShardThreads(Collection<Integer> shards) {
        // ensure consumers for each of our shards
        for (Integer shard : shards) {
            Thread thread = shardThreads.get(shard);
            if (thread != null && thread.isAlive()) {
                continue;
            }

            final int finalI = shard;

            log.info("Starting thread for " + toString() + " shard="  + shard);
            thread = new Thread(() -> {
                try {
                    //noinspection StatementWithEmptyBody
                    while (consumerBlockingPop(finalI)) {
                    }
                } catch (Exception e) {
                    log.error("error consuming", e);
                }
                shardThreads.remove(shard);
                log.info("Shutting down consumer shard thread topic=" + topicConfig.topic + " consumeName=" + name + " shardIndex=" + shard);
            });
//            consumeShardAsync(consumerConfig, finalI);

            shardThreads.put(shard, thread);
            thread.start();
            // add thread shutdown hook to remove it
        }
    }

    private void heartbeat() {
        HeartbeatResult result = db.run((Function<Transaction, HeartbeatResult>) tr -> {
            ImmutableList.Builder<String> removedConsumersB = ImmutableList.builder();

            // look for timed-out running shards
            final TopicConfig tc = topicConfig;

            for (KeyValue keyValue : tr.snapshot().getRange(Range.startsWith(tc.runningShardKeys.pack()))) {
                long popTime = Tuple.fromBytes(keyValue.getValue()).getLong(0);
                if (System.currentTimeMillis() - popTime > 5000) {
                    // time it out
                    tr.clear(keyValue.getKey());
                }
            }

            // look for dead consumers (those with heartbeats older than consumerHeartbeatTimeoutMillis)
            long now = System.currentTimeMillis();
            ImmutableSet.Builder<String> liveConsumersB = ImmutableSet.builder();
            for (KeyValue keyValue : tr.snapshot().getRange(Range.startsWith(tc.heartbeats.pack()))) {
                String consumerName = tc.heartbeats.unpack(keyValue.getKey()).getString(0);
                if (consumerName.equals(name)) {
                    liveConsumersB.add(name);
                    continue;
                }
                long millis = bytesToMillis(keyValue.getValue());

                if (now - millis > tc.consumerHeartbeatTimeoutMillis) {
                    removedConsumersB.add(consumerName);
                } else {
                    liveConsumersB.add(consumerName);
                }
            }

            ImmutableSet<String> liveConsumers = liveConsumersB.build();
            Multimap<String, Integer> originalAssignments = Helpers.fetchAssignments(tr, tc.assignments);

            originalAssignments.keySet().forEach(consumerName -> {
                if (!liveConsumers.contains(consumerName)) {
                    removedConsumersB.add(consumerName);
                }
            });


            // remove dead consumers
            ImmutableList<String> removedConsumers = removedConsumersB.build();
            Multimap<String, Integer> newAssignments = removeConsumers(tr, removedConsumers, originalAssignments);

            // ensure our consumer is added
            newAssignments = Divvy.addConsumer(newAssignments, name, tc.numShards);

            boolean isAssignmentsUpdated = !newAssignments.equals(originalAssignments);
            if (isAssignmentsUpdated) {
                saveAssignments(tr, newAssignments);
            }

            // update our timestamp
            tr.set(tc.heartbeats.pack(name), currentTimeMillisAsBytes());

            // check for de/activation
            boolean isActivated = Helpers.isActivated(tr, tc);

            return new HeartbeatResult(removedConsumers, newAssignments, isAssignmentsUpdated, isActivated);
        });

        if (result.isActivated) {
            ensureShardThreads(result.assignments.get(name));
        }

        if (result.isAssignmentsUpdated) {
            log.debug("Assignments updated, topic=" + topicConfig.topic + " removedConsumers=" + result.removedConsumers + " assignments=" + result.assignments);
        }

    }

    private Multimap<String, Integer> removeConsumers(Transaction tr, Collection<String> names, Multimap<String, Integer> assignments) {
        // remove these consumers
        for (String name : names) {
            assignments = Divvy.removeConsumer(assignments, name, topicConfig.numShards);

            // unregister consumer / remove heartbeat
            tr.clear(topicConfig.heartbeats.pack(name));
        }

        return assignments;
    }

    /**
     * Fetches up to batchSize worth of messages or blocks watching for messages if there are currently none
     *
     * @param shardIndex
     * @return true if we should keep tailing
     */
    private boolean consumerBlockingPop(Integer shardIndex) {
        final TopicConfig tc = topicConfig;

        // casting is here because of a java 8 compiler bug with ambiguous overloads :(
        ConsumeWorkResult workResult = db.run((Function<Transaction, ConsumeWorkResult>) tr -> {
            // make sure we dont read things that are no longer ours
            if (!isMine(tr, shardIndex)) {
                return new ConsumeWorkResult(null, null, false);
            }
            if (!Helpers.isActivated(tr, tc)) {
                return new ConsumeWorkResult(null, null, false);
            }


            // FETCH SOME TUPLES
            DirectorySubspace dataDir = tc.shardData.get(shardIndex);
            List<Envelope> result = new ArrayList<>();
            int numPopped = 0;
            int numSkipped = 0;
            int numErrored = 0;

            for (KeyValue keyValue : tr.snapshot().getRange(Range.startsWith(dataDir.pack()), batchSize)) {
                try {

                    // Deserialize key
                    Tuple value = Tuple.fromBytes(keyValue.getValue());
                    String shardKey = value.getString(0);
                    byte[] runningShardKey = tc.runningShardKeys.pack(shardKey);

                    // ensure we aren't executing messages with the same shard key concurrently
                    boolean isRunning = null != tr.get(runningShardKey).get();
                    if (isRunning) {
                        numSkipped++;
                        continue;
                    }

                    Tuple key = dataDir.unpack(keyValue.getKey());
                    long insertionTime = key.getLong(0);
                    int randomInt = (int) key.getLong(1);

                    byte[] message = value.getBytes(1);

                    tr.clear(keyValue.getKey());
                    tr.addReadConflictKey(keyValue.getKey());
                    numPopped++;

                    Envelope envelope = new Envelope(insertionTime, randomInt, shardKey, shardIndex, message);
                    result.add(envelope);

                    // log that we are running this shardkey and tuple
                    Long popTime = System.currentTimeMillis();
                    tr.set(runningShardKey, Tuple.from(popTime).pack());
                    tr.set(tc.runningData.pack(key), Tuple.from(popTime).pack());

                } catch (Exception e) {
                    // issue with deserialization
                    tr.clear(keyValue.getKey());
                    log.warn("swallowed error during consume, lost message", e);
                    numErrored++;
                }
            }

            tc.incShardMetric(tr, shardIndex, METRIC_POPPED, numPopped);
            tc.incShardMetric(tr, shardIndex, METRIC_SKIPPED, numSkipped);
            tc.incMetric(tr, METRIC_POPPED, numPopped);
            tc.incMetric(tr, METRIC_SKIPPED, numSkipped);
            // this type of deser error is an internal error and very unexpected, not incrementing error metric, thats for consumer function errors

            if (numPopped < batchSize && numSkipped == 0 && numErrored == 0) {
                // exhausted
                byte[] topicWatchKey = tc.shardMetric(shardIndex, TopicConfig.METRIC_INSERTED);
                return new ConsumeWorkResult(tr.watch(topicWatchKey), result, true);
            }

            if (numPopped + numErrored == 0 && numSkipped == batchSize) {
                // TODO: pop some more next time so we don't keep spinning on these
                log.warn("Whole batch was skipped for " + toString());
                try {
                    Thread.sleep(sleepMillisOnAllSkipped);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


            return new ConsumeWorkResult(null, result, true);
        });

        if (!workResult.isStillActive) {
            // shutting down!
            return false;
        }

        for (Envelope envelope : workResult.fetched) {
            if (log.isTraceEnabled()) {
                log.trace("Submitting to executor " + toString() + " " + envelope.toString());
            }

            executorOuter.submit(() -> consumeEnvelope(envelope));
        }

        if (workResult.watch != null) {
            log.debug("Exhausted queue, watching, topic=" + toString() + " shardIndex=" + shardIndex);
            workResult.watch.get();
        } else {
            //optionally sleep
            if (sleepMillisBetweenBatches != 0) {
                try {
                    Thread.sleep(sleepMillisBetweenBatches);
                } catch (InterruptedException e) {
                    return false;
                }
            }
        }

        return true;
    }

    private void consumeEnvelope(Envelope envelope) {
        Exception exception = null;
        long duration = 0;
        boolean isTimedOut = false;
        try {
            long start = System.currentTimeMillis();

            java.util.concurrent.Future<?> task = executorInner.submit(() -> consumer.accept(envelope));
            task.get(5, TimeUnit.SECONDS);

            duration = System.currentTimeMillis() - start;
        } catch (TimeoutException e) {
            isTimedOut = true;
            log.warn("timed out " + toString() + " " + envelope.toString());
        } catch (Exception e) {
            log.error("error during consume, put in errored data folder", e);
            exception = e;
        } finally {
            // TODO: stats/logging hook

            // even if it errors we want to mark it not as running
            final Exception finalException = exception;
            final long finalDuration = duration;
            final boolean finalIsTimedOut = isTimedOut;
            db.run((Function<Transaction, Void>) tr -> {
                final TopicConfig tc = topicConfig;
                final int si = envelope.shardIndex;

                tr.clear(tc.runningData.pack(Tuple.from(envelope.insertionTime, envelope.randomInt)));
                tr.clear(tc.runningShardKeys.pack(envelope.shardKey));

                if (finalException != null) {
                    tr.set(tc.erroredData.pack(Tuple.from(envelope.insertionTime, envelope.randomInt)), Tuple.from(envelope.shardKey, envelope.message, System.currentTimeMillis(), Helpers.prettyStackTrace(finalException, "consumeEnvelope")).pack());
                    tc.incShardMetric(tr, si, METRIC_ERRORED);
                    tc.incMetric(tr, METRIC_ERRORED);
                    tc.incMetric(tr, METRIC_ERRORED_DURATION, (int) finalDuration);
                } else if (finalIsTimedOut) {
                    tc.incShardMetric(tr, si, METRIC_TIMED_OUT);
                    tc.incMetric(tr, METRIC_TIMED_OUT);
                }else {
                    tc.incShardMetric(tr, si, METRIC_ACKED);
                    tc.incMetric(tr, METRIC_ACKED);
                    tc.incMetric(tr, METRIC_ACKED_DURATION, (int) finalDuration);
                }

                return null;
            });
        }
    }

    private boolean isMine(Transaction tr, Integer shardIndex) {
        return fetchAssignment(tr, shardIndex)
                .filter(name::equals).isPresent();
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
        private final boolean isActivated;

        private HeartbeatResult(List<String> removedConsumers, Multimap<String, Integer> assignments, boolean isAssignmentsUpdated, boolean isActivated) {
            this.removedConsumers = removedConsumers;
            this.assignments = assignments;
            this.isAssignmentsUpdated = isAssignmentsUpdated;
            this.isActivated = isActivated;
        }
    }

}
