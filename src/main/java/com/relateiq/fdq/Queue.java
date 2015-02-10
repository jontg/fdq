package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.KeyValue;
import com.foundationdb.MutationType;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.AsyncIterable;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.directory.Directory;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.Tuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Created by mbessler on 2/6/15.
 */
public class Queue {

    private final Database db;
    private final Random random = new Random();
    private final ConcurrentHashMap<List<String>, DirectorySubspace> directories = new ConcurrentHashMap<>();

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

        DirectorySubspace dataDir = getDirectory(topic, "data");
        byte[] topicWatchKey = getTopicWatchKey(topic);

        db.run((Function<Transaction, Void>) tr -> {
            tr.set(dataDir.pack(Tuple.from(System.nanoTime(), random.nextInt())), Tuple.from(shardKey, value).pack());
            tr.mutate(MutationType.ADD, topicWatchKey, intToByteArray(1));
            return null;
        });
    }

    private byte[] getTopicWatchKey(String topic) {
        return getDirectory(topic, "metrics").pack(Tuple.from("inserted"));
    }

    private DirectorySubspace getDirectory(String topic, String subdir) {
        List<String> key = Arrays.asList(topic, subdir);
        DirectorySubspace result = directories.get(key);
        if (result != null) {
            return result;
        }
        result = db.run((Function<Transaction, DirectorySubspace>) tr -> DirectoryLayer.getDefault().createOrOpen(tr, key).get());
        directories.putIfAbsent(key, result);
        return result;
    }



    public void tail(final String topic, Consumer<Envelope> consumer) {
        DirectorySubspace dataDir = getDirectory(topic, "data");
        byte[] topicWatchKey = getTopicWatchKey(topic);

        while (true) {
            tailWork(topic, dataDir, topicWatchKey, consumer);
        }

    }


    private void tailWork(String topic, DirectorySubspace dataDir, byte[] topicWatchKey, Consumer<Envelope> consumer) {


        // casting is here because of a java 8 compiler bug with ambiguous overloads :(
        Future<Void> watch = db.run((Function<Transaction, Future<Void>>) tr -> tr.watch(topicWatchKey));
//        watch.map((Function<Void, Void>) aVoid -> {
//            return null;
//        });

        watch.get();

        // casting is here because of a java 8 compiler bug with ambiguous overloads :(
        List<Envelope> fetched = db.run((Function<Transaction, List<Envelope>>) tr -> {
            List<Envelope> result = new ArrayList<>();
            for (KeyValue keyValue : tr.snapshot().getRange(Range.startsWith(dataDir.pack()))) {
                tr.addReadConflictKey(keyValue.getKey());
                tr.clear(keyValue.getKey());

                try {

                    Tuple key = dataDir.unpack(keyValue.getKey());
                    Tuple value = Tuple.fromBytes(keyValue.getValue());
//                System.out.println("kv: " + key.getLong(2) + " " + key.getLong(3) + " : " + new String(keyValue.getValue()));

                    long insertionTime = key.getLong(0);

                    String shardKey = value.getString(0);
                    byte[] message = value.getBytes(1);
                    result.add(new Envelope(insertionTime, shardKey, message));
                } catch (Exception e){
                    // issue with deserialization
                    System.out.println("error deser: " + e.toString());
                }
            }

            return result;
        });

        for (Envelope envelope : fetched) {
            consumer.accept(envelope);
        }
    }


}
