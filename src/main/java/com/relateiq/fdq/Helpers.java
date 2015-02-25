package com.relateiq.fdq;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;


/**
 * Created by mbessler on 2/19/15.
 */
public class Helpers {

    public static final Charset CHARSET = Charset.forName("UTF-8");
    public static final byte[] ONE = intToByteArray(1);
    public static final int NUM_EXECUTORS = 10;
    public static final int MOD_HASH_ITERATIONS_QUEUE_SHARDING = 1;
    public static final int MOD_HASH_ITERATIONS_EXECUTOR_SHARDING = 2;
    public static final String DIR_METRICS = "metrics";
    public static final String DIR_CONFIG = "config";
    public static final String DIR_ASSIGNMENTS = "assignments";
    public static final String DIR_HEARTBEATS = "heartbeats";
    public static final String DIR_CONSUMERS = "consumers";

    public static final int NUM_SHARDS = 24;  // todo: make this configurable

    private static final HashFunction hashFunction = Hashing.goodFastHash(32);

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

    public static byte[] currentTimeMillisAsBytes() {
        return ByteBuffer.allocate(8).putLong(System.currentTimeMillis()).array();
    }

    public static long bytesToMillis(byte[] in) {
        return ByteBuffer.wrap(in).getLong();
    }

}
