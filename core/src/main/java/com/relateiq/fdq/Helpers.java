package com.relateiq.fdq;

import com.foundationdb.KeyValue;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.TransactionContext;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Created by mbessler on 2/19/15.
 */
public class Helpers {

    public static final Charset CHARSET = Charset.forName("UTF-8");
    public static final byte[] ONE = intToByteArray(1);
    public static final int MOD_HASH_ITERATIONS_QUEUE_SHARDING = 1;
    public static final int MOD_HASH_ITERATIONS_EXECUTOR_SHARDING = 2;
    public static final DirectoryLayer DIRECTORY_LAYER = DirectoryLayer.getDefault();
    public static final String DEACTIVATED = "deactivated";

    private static final HashFunction hashFunction = Hashing.goodFastHash(32);
    public static final byte[] NULL = {0};

    public static int toInt(byte[] in){
        if (in == null){ return 0;}
        ByteBuffer bb = ByteBuffer.wrap(in);
        bb.order( ByteOrder.LITTLE_ENDIAN);
        return bb.getInt();
    }

    public static long toLong(byte[] in){
        if (in == null){ return 0l;}
        ByteBuffer bb = ByteBuffer.wrap(in);
        bb.order( ByteOrder.LITTLE_ENDIAN);
        return bb.getLong();
    }

    public static byte[] intToByteArray(int i) {
        final ByteBuffer bb = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(i);
        return bb.array();
    }

    public static Integer modHash(String shardKey, int shards, int iterations) {
        HashCode result = hashFunction.hashString(shardKey, Helpers.CHARSET);
        int iterationsLeft = iterations -1 ;
        for (int i = 0; i < iterationsLeft; i++) {
            result = hashFunction.hashBytes(result.asBytes());
        }

        return Math.abs(result.asInt()) % shards;
    }

    public static byte[] currentTimeMillisAsBytes() {
        return ByteBuffer.allocate(8).putLong(System.currentTimeMillis()).array();
    }

    public static long bytesToMillis(byte[] in) {
        return ByteBuffer.wrap(in).getLong();
    }

    public static TopicConfig createTopicConfig(TransactionContext tr, String topic) {
        // init directories
        DirectorySubspace config = mkdirp(tr, topic, "config");
        DirectorySubspace assignments = mkdirp(tr, topic, "config", "assignments");
        DirectorySubspace heartbeats = mkdirp(tr, topic, "config", "heartbeats");

        DirectorySubspace topicMetrics = mkdirp(tr, topic, "metrics", "topic");

        DirectorySubspace erroredData = mkdirp(tr, topic, "data", "errored");
        DirectorySubspace runningData = mkdirp(tr, topic, "data", "running");
        DirectorySubspace runningShardKeys = mkdirp(tr, topic, "data", "runningShardKeys");

        ImmutableMap.Builder<Integer, DirectorySubspace> shardMetrics = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, DirectorySubspace> shardData = ImmutableMap.builder();
        for (int i = 0; i < TopicConfig.DEFAULT_NUM_SHARDS; i++) {
            shardMetrics.put(i, mkdirp(tr, topic, "metrics", "shards", "" + (Integer) i));
            shardData.put(i, mkdirp(tr, topic, "data", "shards", "" + (Integer) i));
        }
        return new TopicConfig(topic, config, assignments, heartbeats, topicMetrics, erroredData, runningData, runningShardKeys, shardMetrics.build(), shardData.build());
    }

    /**
     *
     * @param tr the transaction
     * @param topicAssignmentDirectory
     * @return the assignments, a multimap of shardIndexes indexed by consumerName
     */
    public static Multimap<String, Integer> fetchAssignments(Transaction tr, DirectorySubspace topicAssignmentDirectory) {
        ImmutableMultimap.Builder<String, Integer> builder = ImmutableMultimap.builder();

        for (KeyValue keyValue : tr.getRange(Range.startsWith(topicAssignmentDirectory.pack()))) {
            Integer shardIndex = (int) topicAssignmentDirectory.unpack(keyValue.getKey()).getLong(0);
            String consumerName = new String(keyValue.getValue(), CHARSET);
            builder.put(consumerName, shardIndex);
        }

        return builder.build();
    }

    public static DirectorySubspace mkdirp(TransactionContext tr, String... strings) {
        return DIRECTORY_LAYER.createOrOpen(tr, Arrays.asList(strings)).get();
    }

    public static void rmdir(TransactionContext tr, String... strings) {
        DIRECTORY_LAYER.removeIfExists(tr, Arrays.asList(strings)).get();
    }

    public static boolean isActivated(Transaction tr, TopicConfig topicConfig) {
        return tr.get(topicConfig.config.pack(DEACTIVATED)).get() == null;
    }

    static String prettyStackTrace(Exception e, String stopAtMethodName) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append(element.toString()).append("\n");
            if (element.getMethodName().equals(stopAtMethodName)) {
                break;
            }
        }

        return sb.toString();
    }

    static ThreadPoolExecutor createExecutor() {
        // TODO: configurable # of executors
        return new ThreadPoolExecutor(TopicConsumer.DEFAULT_NUM_EXECUTORS, TopicConsumer.DEFAULT_NUM_EXECUTORS,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(TopicConsumer.EXECUTOR_QUEUE_SIZE));
    }
}
