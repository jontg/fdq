package com.relateiq.fdq;

import com.foundationdb.KeyValue;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.TransactionContext;
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

import static com.relateiq.fdq.DirectoryCache.mkdirp;


/**
 * Created by mbessler on 2/19/15.
 */
public class Helpers {

    public static final Charset CHARSET = Charset.forName("UTF-8");
    public static final byte[] ONE = intToByteArray(1);
    public static final int MOD_HASH_ITERATIONS_QUEUE_SHARDING = 1;
    public static final int MOD_HASH_ITERATIONS_EXECUTOR_SHARDING = 2;

    private static final HashFunction hashFunction = Hashing.goodFastHash(32);

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
        DirectorySubspace assignments = mkdirp(tr, topic, "config", "assignments");
        DirectorySubspace heartbeats = mkdirp(tr, topic, "config", "heartbeats");
        DirectorySubspace runningData = mkdirp(tr, topic, "config", "runningData");
        DirectorySubspace runningShardKeys = mkdirp(tr, topic, "config", "runningShardKeys");
        DirectorySubspace topicMetrics = mkdirp(tr, topic, "config", "topicMetrics");
        DirectorySubspace erroredData = mkdirp(tr, topic, "config", "erroredData");

        ImmutableMap.Builder<Integer, DirectorySubspace> shardMetrics = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, DirectorySubspace> shardData = ImmutableMap.builder();
        for (int i = 0; i < TopicConfig.DEFAULT_NUM_SHARDS; i++) {
            shardMetrics.put(i, mkdirp(tr, topic, "metrics", "" + (Integer) i));
            shardData.put(i, mkdirp(tr, topic, "data", "" + (Integer) i));
        }
        return new TopicConfig(topic, assignments, heartbeats, topicMetrics, erroredData, runningData, runningShardKeys, shardMetrics.build(), shardData.build());
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
}
