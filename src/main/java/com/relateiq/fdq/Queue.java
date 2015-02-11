package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.KeyValue;
import com.foundationdb.MutationType;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.Tuple;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Created by mbessler on 2/6/15.
 */
public class Queue {

    public static final Charset CHARSET = Charset.forName("UTF-8");
    private final Database db;
    private final Random random = new Random();
    private final ConcurrentHashMap<List<String>, DirectorySubspace> directories = new ConcurrentHashMap<>();
    private static final HashFunction hashFunction = Hashing.goodFastHash(32);

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

        HashCode hash = hashFunction.hashString(shardKey, CHARSET);
        String partitionIndex = "" + Math.abs(hash.asInt()) % 32;

        DirectorySubspace dataDir = getCachedDirectory(Arrays.asList(topic, "data", partitionIndex));
        byte[] topicWatchKey = getTopicWatchKey(topic, partitionIndex);

        db.run((Function<Transaction, Void>) tr -> {
            tr.set(dataDir.pack(Tuple.from(System.nanoTime(), random.nextInt())), Tuple.from(shardKey, value).pack());
            tr.mutate(MutationType.ADD, topicWatchKey, intToByteArray(1));
            return null;
        });
    }

    private byte[] getTopicWatchKey(String topic, String partitionIndex) {
        return getCachedDirectory(Arrays.asList(topic, "metrics", partitionIndex)).pack(Tuple.from("inserted"));
    }

    private DirectorySubspace getCachedDirectory(List<String> strings) {
        List<String> key = strings;
        DirectorySubspace result = directories.get(key);
        if (result != null) {
            return result;
        }
        result = db.run((Function<Transaction, DirectorySubspace>) tr -> DirectoryLayer.getDefault().createOrOpen(tr, key).get());
        directories.putIfAbsent(key, result);
        return result;
    }



    public void tail(final String topic, Consumer<Envelope> consumer) {

        ExecutorService executor = Executors.newFixedThreadPool(10);

        // ensure tailers for each of our partitions
        for (int i = 0; i < 32; i++) {
            final int finalI = i;
            new Thread(() -> tailPartition(topic, "" + finalI, executor, consumer)).start();
        }
    }

    public void tailPartition(final String topic, final String partitionIndex, ExecutorService executor, Consumer<Envelope> consumer) {
        DirectorySubspace dataDir = getCachedDirectory(Arrays.asList(topic, "data", partitionIndex));
        byte[] topicWatchKey = getTopicWatchKey(topic, partitionIndex);

        while (tailWork(dataDir, topicWatchKey, executor, consumer)) {

        }
    }


    private boolean tailWork(DirectorySubspace dataDir, byte[] topicWatchKey, ExecutorService executor, Consumer<Envelope> consumer) {
        // ensure we still have this token, otherwise return false to stop tailing this token
        

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
            executor.submit(() -> consumer.accept(envelope));
        }

        return true;
    }


}
