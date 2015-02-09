package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.KeyValue;
import com.foundationdb.MutationType;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.AsyncIterable;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.tuple.Tuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by mbessler on 2/6/15.
 */
public class Queue {

    private final Database db;
    private final Random random = new Random();

    public Queue(Database db) {
        this.db = db;
    }

    public static byte[] intToByteArray(int i) {
        final ByteBuffer bb = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(i);
        return bb.array();
    }

    public void enqueue(final String topic, final String value) {
        db.run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                tr.set(Tuple.from(topic, "q", System.nanoTime(), random.nextInt()).pack(), Tuple.from(value).pack());
                tr.mutate(MutationType.ADD, topicWatchKey(topic), intToByteArray(1));
                return null;
            }
        });
    }

    private byte[] topicWatchKey(String topic) {
        return Tuple.from(topic, "inserted").pack();
    }

    public void tail(final String topic) {

        while (true) {
            tailWork(topic);
        }

    }

    private void tailWork(String topic) {
        System.out.println("watching...");
        Future<Void> watch = db.run((Function<Transaction, Future<Void>>) tr -> tr.watch(topicWatchKey(topic)));
        watch.map((Function<Void, Void>) aVoid -> {
            System.out.println("hit");
            return null;
        });

        watch.get();

        List<String> fetched = db.run((Function<Transaction, List<String>>) tr -> {
            List<String> result = new ArrayList<>();
            for (KeyValue keyValue : tr.snapshot().getRange(Range.startsWith(Tuple.from(topic, "q").pack()))) {
                Tuple key = Tuple.fromBytes(keyValue.getKey());
//                System.out.println("kv: " + key.getLong(2) + " " + key.getLong(3) + " : " + new String(keyValue.getValue()));
                long nanos = key.getLong(2);
                tr.addReadConflictKey(keyValue.getKey());
                tr.clear(keyValue.getKey());
                result.add(((System.nanoTime() - nanos) / 1000000) + " " + new String(keyValue.getValue()));
            }
            return result;
        });

        for (String s : fetched) {
            System.out.println(s);
        }

        System.out.println("done watching...");
    }


}
