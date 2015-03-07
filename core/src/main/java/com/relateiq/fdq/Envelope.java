package com.relateiq.fdq;

import java.util.Arrays;

/**
 * Created by mbessler on 2/10/15.
 */
public class Envelope {
    public final long insertionTime; // client time (use NTP!)
    public final int randomInt;
    public final String shardKey;
    public final byte[] message;
    public final int shardIndex;

    public Envelope(long insertionTime, int randomInt, String shardKey, int shardIndex, byte[] message) {
        this.insertionTime = insertionTime;
        this.randomInt = randomInt;
        this.shardKey = shardKey;
        this.message = message;
        this.shardIndex = shardIndex;
    }

    @Override
    public String toString() {
        return "Envelope{" +
                "insertionTime=" + insertionTime +
                ", randomInt=" + randomInt +
                ", shardKey='" + shardKey + '\'' +
                ", messageLength=" + message.length +
                ", shardIndex=" + shardIndex +
                '}';
    }
}
