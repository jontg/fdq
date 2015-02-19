package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class QueueTest {
    public static final Logger log = LoggerFactory.getLogger(QueueTest.class);

    @Test
    public void testAssignments() {
        String TOPIC = "" + (new Random()).nextLong();
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Queue q = new Queue(db);

        Multimap<String, Integer> assignments = HashMultimap.create();
        assignments.putAll("a", IntStream.range(0, 32).mapToObj(x -> x).collect(toSet()));
        db.run((Function<Transaction, Void>)tr -> {q.saveAssignments(tr, TOPIC,  assignments);
            log.debug(q.fetchAssignments(tr, TOPIC).toString());
            return null;});
    }

    @Test
    public void tester() throws InterruptedException {
        String TOPIC = "" + (new Random()).nextLong();
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Queue q = new Queue(db);
        q.clearAssignments(TOPIC);

        Set<String> rcvd = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

        new Thread(() -> q.tail(TOPIC, "a", e -> rcvd.add(new String(e.message)))).start();
        new Thread(() -> q.tail(TOPIC, "b", e -> rcvd.add(new String(e.message)))).start();
        new Thread(() -> q.tail(TOPIC, "c", e -> rcvd.add(new String(e.message)))).start();


        IntStream.range(0, 100).forEach(i -> q.enqueue(TOPIC, "" + i, ("qwerty " + i).getBytes()));

        // flaky, better to block somehow on rcv
        Thread.sleep(1500);

        assertEquals(100, rcvd.size());

        q.nukeTopic(TOPIC);


//        db.run(new Function<Transaction, Void>() {
//            @Override
//            public Void apply(Transaction tr) {
//                for (KeyValue keyValue : tr.getRange(Range.startsWith(Tuple.from("asdf").pack()))) {
//                        log.debug("hmm: " + new String(keyValue.getKey()) + " : " + new String(keyValue.getValue()));
//                }
//                return null;
//            }
//        });

//        Thread.sleep(1000);

    }


}