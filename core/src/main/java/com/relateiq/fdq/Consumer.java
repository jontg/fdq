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
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
public class Consumer {
    public static final Logger log = LoggerFactory.getLogger(Consumer.class);
    public static final int EXECUTOR_QUEUE_SIZE = 1000;
    public static final int HEARTBEAT_MILLIS = 2000;
    private final Database db;

    public Consumer(Database db) {
        this.db = db;
    }

    private static String prettyStackTrace(Exception e) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append(element.toString()).append("\n");
            if (element.getMethodName().equals("consumerWrapper")) {
                break;
            }
        }

        return sb.toString();
    }

    private static <T> T timedCall(ExecutorService executor, Callable<T> c, long timeout, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException {
        FutureTask<T> task = new FutureTask<T>(c);
        executor.execute(task);
        return task.get(timeout, timeUnit);
    }

    /**
     *
     * @param tr the transaction
     * @param consumerConfig
     * @param shardIndex the
     * @return
     */
    private Optional<String> fetchAssignment(Transaction tr, ConsumerConfig consumerConfig, Integer shardIndex) {
        byte[] bytes = tr.get(consumerConfig.topicConfig.shardAssignmentKey(shardIndex)).get();
        if (bytes == null) {
            return Optional.empty();
        }
        return Optional.of(new String(bytes, Helpers.CHARSET));
    }

    void saveAssignments(Transaction tr, ConsumerConfig consumerConfig, Multimap<String, Integer> assignments) {
        for (Map.Entry<String, Collection<Integer>> entry : assignments.asMap().entrySet()) {
            for (Integer integer : entry.getValue()) {
                tr.set(consumerConfig.topicConfig.shardAssignmentKey(integer), entry.getKey().getBytes(Helpers.CHARSET));
            }
        }
    }

    public ConsumerConfig consume(final String topic, String consumerName, java.util.function.Consumer<Envelope> consumer) {
        ConsumerConfig consumerConfig = initConsumer(topic, consumerName, consumer);

        // start heartbeat thread
        new Thread(() -> {
            while (true) {
                if (log.isTraceEnabled()) {
                    log.trace("heartbeat " + consumerConfig.toString());
                }
                heartbeat(consumerConfig);
                try {
                    Thread.sleep(HEARTBEAT_MILLIS);
                } catch (InterruptedException e) {
                    log.info("heartbeat interrupted, closing consume topic=" + topic + " consumerName=" + consumerName);
                    break;
                }
            }
        }).start();

        return consumerConfig;
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
                    //noinspection StatementWithEmptyBody
                    while (consumeWork(consumerConfig, finalI)) {}
                } catch (Exception e) {
                    log.error("error consuming", e);
                }
                consumerConfig.shardThreads.remove(shard);
                log.info("Shutting down consumer shard thread topic=" + consumerConfig.topicConfig.topic + " consumeName=" + consumerConfig.name + " shardIndex=" + shard);
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
            TopicConfig topicConfig = Helpers.createTopicConfig(tr, topic);

            // register consumer
            tr.set(topicConfig.heartbeats.pack(consumerName), currentTimeMillisAsBytes());

            // TODO: configurable # of executors

            return new ConsumerConfig(topicConfig,
                    consumerName,
                    consumer,
                    new ThreadPoolExecutor(ConsumerConfig.DEFAULT_NUM_EXECUTORS, ConsumerConfig.DEFAULT_NUM_EXECUTORS,
                            0L, TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(EXECUTOR_QUEUE_SIZE)),
                    new ThreadPoolExecutor(ConsumerConfig.DEFAULT_NUM_EXECUTORS, ConsumerConfig.DEFAULT_NUM_EXECUTORS,
                            0L, TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(EXECUTOR_QUEUE_SIZE)));
        });
    }

    private void heartbeat(ConsumerConfig consumerConfig) {

        HeartbeatResult result = db.run((Function<Transaction, HeartbeatResult>) tr -> heartbeat(tr, consumerConfig));

        if (result.isActivated) {
            ensureShardThreads(consumerConfig, result.assignments.get(consumerConfig.name));
        }

        if (result.isAssignmentsUpdated) {
            log.debug("Assignments updated, topic=" + consumerConfig.topicConfig.topic + " removedConsumers=" + result.removedConsumers + " assignments=" + result.assignments);
        }

    }

    /**
     * This method is responsible for maintaing proper state
     * <p>
     * This will:
     * 1. clean out dead consumers
     * 2. ensure this consumer is registered
     *
     * @param tr
     * @param consumerConfig
     * @return
     */
    private HeartbeatResult heartbeat(Transaction tr, ConsumerConfig consumerConfig) {
        ImmutableList.Builder<String> removedConsumersB = ImmutableList.builder();

        // look for timed-out running shards
        final TopicConfig tc = consumerConfig.topicConfig;

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
            if (consumerName.equals(consumerConfig.name)) {
                liveConsumersB.add(consumerConfig.name);
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
        Multimap<String, Integer> newAssignments = removeConsumers(tr, removedConsumers, consumerConfig, originalAssignments);

        // ensure our consumer is added
        newAssignments = Divvy.addConsumer(newAssignments, consumerConfig.name, tc.numShards);

        boolean isAssignmentsUpdated = !newAssignments.equals(originalAssignments);
        if (isAssignmentsUpdated) {
            saveAssignments(tr, consumerConfig, newAssignments);
        }

        // update our timestamp
        tr.set(tc.heartbeats.pack(consumerConfig.name), currentTimeMillisAsBytes());

        // check for de/activation
        boolean isActivated = isActivated(tr, tc);

        return new HeartbeatResult(removedConsumers, newAssignments, isAssignmentsUpdated, isActivated);
    }

    private Multimap<String, Integer> removeConsumers(Transaction tr, Collection<String> names, ConsumerConfig consumerConfig, Multimap<String, Integer> assignments) {
        // fetch currentAssignments from fdb

        // remove these consumers
        for (String name : names) {
            assignments = Divvy.removeConsumer(assignments, name, consumerConfig.topicConfig.numShards);

            // unregister consumer / remove heartbeat
            tr.clear(consumerConfig.topicConfig.heartbeats.pack(name));
        }

        return assignments;
    }

    private boolean consumeWork(ConsumerConfig consumerConfig, Integer shardIndex) {
        final TopicConfig tc = consumerConfig.topicConfig;

        // casting is here because of a java 8 compiler bug with ambiguous overloads :(
        ConsumeWorkResult workResult = db.run((Function<Transaction, ConsumeWorkResult>) tr -> {
            // make sure we dont read things that are no longer ours
            if (!isMine(tr, consumerConfig, shardIndex)) {
                return new ConsumeWorkResult(null, null, false);
            }
            if (!isActivated(tr, tc)) {
                return new ConsumeWorkResult(null, null, false);
            }


            // FETCH SOME TUPLES
            DirectorySubspace dataDir = tc.shardData.get(shardIndex);
            List<Envelope> result = new ArrayList<>();
            int numPopped = 0;
            int numSkipped = 0;
            int numErrored = 0;

            for (KeyValue keyValue : tr.snapshot().getRange(Range.startsWith(dataDir.pack()), consumerConfig.batchSize)) {
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

            if (numPopped < consumerConfig.batchSize && numSkipped == 0 && numErrored == 0) {
                // exhausted
                byte[] topicWatchKey = tc.shardMetric(shardIndex, TopicConfig.METRIC_INSERTED);
                return new ConsumeWorkResult(tr.watch(topicWatchKey), result, true);
            }

            if (numPopped + numErrored == 0 && numSkipped == consumerConfig.batchSize) {
                // TODO: pop some more next time so we don't keep spinning on these
                log.warn("Whole batch was skipped for " + consumerConfig.toString());
                try {
                    Thread.sleep(consumerConfig.sleepMillisOnAllSkipped);
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
                log.trace("Submitting to executorOuter " + consumerConfig.toString() + " " + envelope.toString());
            }

            consumerConfig.executorOuter.submit(() -> consumerWrapper(consumerConfig, envelope));
        }

        if (workResult.watch != null) {
            log.debug("Exhausted queue, watching, executorOuter topic=" + consumerConfig.toString() + " shardIndex=" + shardIndex);
            workResult.watch.get();
        } else {
            //optionally sleep
            if (consumerConfig.sleepMillisBetweenBatches != 0) {
                try {
                    Thread.sleep(consumerConfig.sleepMillisBetweenBatches);
                } catch (InterruptedException e) {
                    return false;
                }
            }
        }

        return true;
    }

    private void consumerWrapper(ConsumerConfig consumerConfig, Envelope envelope) {
        Exception exception = null;
        long duration = 0;
        boolean isTimedOut = false;
        try {
            long start = System.currentTimeMillis();

            java.util.concurrent.Future<?> task = consumerConfig.executorInner.submit(() -> consumerConfig.consumer.accept(envelope));
            task.get(5, TimeUnit.SECONDS);

            duration = System.currentTimeMillis() - start;
        } catch (TimeoutException e) {
            isTimedOut = true;
            log.warn("timed out " + consumerConfig.toString() + " " + envelope.toString());
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
                final TopicConfig tc = consumerConfig.topicConfig;
                final int si = envelope.shardIndex;

                tr.clear(tc.runningData.pack(Tuple.from(envelope.insertionTime, envelope.randomInt)));
                tr.clear(tc.runningShardKeys.pack(envelope.shardKey));

                if (finalException != null) {
                    tr.set(tc.erroredData.pack(Tuple.from(envelope.insertionTime, envelope.randomInt)), Tuple.from(envelope.shardKey, envelope.message, System.currentTimeMillis(), prettyStackTrace(finalException)).pack());
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

    private boolean isMine(Transaction tr, ConsumerConfig consumerConfig, Integer shardIndex) {
        return fetchAssignment(tr, consumerConfig, shardIndex)
                .filter(consumerConfig.name::equals).isPresent();
    }

    private boolean isActivated(Transaction tr, TopicConfig topicConfig) {
        return tr.get(topicConfig.config.pack("deactivated")).get() == null;
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
