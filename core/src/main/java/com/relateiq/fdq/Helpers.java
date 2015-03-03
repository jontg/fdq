package com.relateiq.fdq;

import com.foundationdb.TransactionContext;
import com.foundationdb.directory.DirectorySubspace;
import com.google.common.collect.ImmutableMap;
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
    public static final String DIR_METRICS = "metrics";
    public static final String DIR_CONFIG = "config";
    public static final String DIR_ASSIGNMENTS = "assignments";
    public static final String DIR_HEARTBEATS = "heartbeats";
    public static final String DIR_RUNNING_DATA = "runningData";
    public static final String DIR_RUNNING_SHARD_KEYS = "runningShardKeys";

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

    public static String[] getTopicShardDataPath(String topic, Integer shardIndex) {
        return new String[]{topic, "data", "" + shardIndex};
    }

    public static String[] getTopicShardMetricPath(String topic, Integer shardIndex) {
        return new String[]{topic, DIR_METRICS, "" + shardIndex};
    }

    public static String[] getTopicAssignmentPath(String topic) {
        return new String[]{topic, DIR_CONFIG, DIR_ASSIGNMENTS};
    }

    public static String[] getTopicHeartbeatPath(String topic) {
        return new String[]{topic, DIR_CONFIG, DIR_HEARTBEATS};
    }

    public static String[] getTopicRunningDataPath(String topic) {
        return new String[]{topic, DIR_CONFIG, DIR_RUNNING_DATA};
    }

    public static String[] getTopicRunningShardKeysPath(String topic) {
        return new String[]{topic, DIR_CONFIG, DIR_RUNNING_SHARD_KEYS};
    }

    public static byte[] currentTimeMillisAsBytes() {
        return ByteBuffer.allocate(8).putLong(System.currentTimeMillis()).array();
    }

    public static long bytesToMillis(byte[] in) {
        return ByteBuffer.wrap(in).getLong();
    }

    public static TopicConfig createTopicConfig(TransactionContext tr, String topic) {
        // init directories
        DirectorySubspace assignments = mkdirp(tr, getTopicAssignmentPath(topic));
        DirectorySubspace heartbeats = mkdirp(tr, getTopicHeartbeatPath(topic));
        DirectorySubspace runningData = mkdirp(tr, getTopicRunningDataPath(topic));
        DirectorySubspace runningShardKeys = mkdirp(tr, getTopicRunningShardKeysPath(topic));
        DirectorySubspace topicMetrics = mkdirp(tr, getTopicRunningShardKeysPath(topic));

        ImmutableMap.Builder<Integer, DirectorySubspace> shardMetrics = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, DirectorySubspace> shardData = ImmutableMap.builder();
        for (int i = 0; i < TopicConfig.DEFAULT_NUM_SHARDS; i++) {
            shardMetrics.put(i, mkdirp(tr, getTopicShardMetricPath(topic, i)));
            shardData.put(i, mkdirp(tr, getTopicShardDataPath(topic, i)));
        }
        return new TopicConfig(topic, assignments, heartbeats, topicMetrics, runningData, runningShardKeys, shardMetrics.build(), shardData.build());
    }
}
