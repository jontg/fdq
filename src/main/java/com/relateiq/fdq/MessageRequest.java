package com.relateiq.fdq;

import java.util.Arrays;

/**
 * Created by mbessler on 2/10/15.
 */
public class MessageRequest {
    public final String shardKey;
    public final byte[] message;

    public MessageRequest(String shardKey, byte[] message) {
        this.shardKey = shardKey;
        this.message = message;
    }

    @Override
    public String toString() {
        return "Message{" +
                ", shardKey='" + shardKey + '\'' +
                ", message=" + Arrays.toString(message) +
                '}';
    }
}
