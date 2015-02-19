package com.relateiq.fdq;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by mbessler on 2/19/15.
 */
public class Helpers {
    private static final HashFunction hashFunction = Hashing.goodFastHash(32);

    public static byte[] intToByteArray(int i) {
        final ByteBuffer bb = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(i);
        return bb.array();
    }

    public static Integer modHash(String shardKey, int shards, int iterations) {
        HashCode result = hashFunction.hashString(shardKey, Queue.CHARSET);
        int iterationsLeft = iterations -1 ;
        for (int i = 0; i < iterationsLeft; i++) {
            result = hashFunction.hashBytes(result.asBytes());
        }

        return Math.abs(result.asInt()) % shards;
    }
}
