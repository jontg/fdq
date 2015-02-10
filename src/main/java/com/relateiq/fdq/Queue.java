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
import java.util.function.Consumer;

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

    public void enqueue(final String topic, final String shardKey, final byte[] value) {
        db.run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                tr.set(Tuple.from(topic, "q", System.nanoTime(), random.nextInt()).pack(), Tuple.from(shardKey, value).pack());
                tr.mutate(MutationType.ADD, topicWatchKey(topic), intToByteArray(1));
                return null;
            }
        });
    }

    private byte[] topicWatchKey(String topic) {
        return Tuple.from(topic, "inserted").pack();
    }

    public void tail(final String topic, Consumer<Envelope> consumer) {

        while (true) {
            tailWork(topic, consumer);
        }

    }


    private void tailWork(String topic, Consumer<Envelope> consumer) {

//        System.out.println("watching...");

        Future<Void> watch = db.run((Function<Transaction, Future<Void>>) tr -> tr.watch(topicWatchKey(topic)));
        watch.map((Function<Void, Void>) aVoid -> {
//            System.out.println("hit");
            return null;
        });

        watch.get();

        List<Envelope> fetched = db.run((Function<Transaction, List<Envelope>>) tr -> {
            List<Envelope> result = new ArrayList<>();
            for (KeyValue keyValue : tr.snapshot().getRange(Range.startsWith(Tuple.from(topic, "q").pack()))) {
                tr.addReadConflictKey(keyValue.getKey());
                tr.clear(keyValue.getKey());

                Tuple key = Tuple.fromBytes(keyValue.getKey());
                Tuple value = Tuple.fromBytes(keyValue.getValue());
//                System.out.println("kv: " + key.getLong(2) + " " + key.getLong(3) + " : " + new String(keyValue.getValue()));

                long insertionTime = key.getLong(2);
                String shardKey = value.getString(0);
                byte[] message = value.getBytes(1);
                result.add(new Envelope(insertionTime, shardKey , message));
            }

            return result;
        });

        for (Envelope envelope : fetched) {
            consumer.accept(envelope);
        }
    }


}
