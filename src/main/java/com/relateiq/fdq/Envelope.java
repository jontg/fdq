package com.relateiq.fdq;

/**
 * Created by mbessler on 2/10/15.
 */
public class Envelope {
    public long insertionTime; // client time (use NTP!)
    public String shardKey;
    public byte[] message;

    public Envelope(long insertionTime, String shardKey, byte[] message) {
        this.insertionTime = insertionTime;
        this.shardKey = shardKey;
        this.message = message;
    }
}
