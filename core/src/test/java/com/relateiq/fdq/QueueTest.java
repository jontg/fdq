package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class QueueTest {
    public static final Logger log = LoggerFactory.getLogger(QueueTest.class);
    public static final Random RANDOM = new Random();

//    @Test
//    public void testAssignments() {
//        String TOPIC = "" + RANDOM.nextLong();
//        FDB fdb = FDB.selectAPIVersion(300);
//        Database db = fdb.open();
//
//        Consumer c = new Consumer(db);
//
//        c.nukeTopic(TOPIC);
//        Multimap<String, Integer> assignments = HashMultimap.create();
//        assignments.putAll("a", IntStream.range(0, 32).mapToObj(x -> x).collect(toSet()));
//        db.run((Function<Transaction, Void>)tr -> {
//
//            c.saveAssignments(tr, directories, assignments);
//            log.debug(c.fetchAssignments(tr, directory(tr, getTopicAssignmentPath(TOPIC))).toString());
//            return null;});
//    }

    @Test
    public void tester() throws InterruptedException {
        String TOPIC = "" + RANDOM.nextLong();
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Consumer c = new Consumer(db);
        c.nukeTopic(TOPIC);
        TopicProducer p = new Producer(db).createProducer(TOPIC);

        Multiset<String> rcvd = ConcurrentHashMultiset.create();

        log.debug("creating consumers");
        new Thread(() -> c.consume(TOPIC, "a", e -> rcvd.add(new String(e.message)))).start();
        new Thread(() -> c.consume(TOPIC, "b", e -> rcvd.add(new String(e.message)))).start();
        new Thread(() -> c.consume(TOPIC, "c", e -> rcvd.add(new String(e.message)))).start();

        log.debug("adding serially");
        long start = System.currentTimeMillis();
        IntStream.range(0, 100).forEach(i -> p.produce("" + i, ("qwerty " + i).getBytes()));
        log.debug("adding serially took " + (System.currentTimeMillis() - start));

        log.debug("adding batch");
        start = System.currentTimeMillis();
        List<MessageRequest> reqs = IntStream.range(0, 100).mapToObj(i -> new MessageRequest("" + i, ("qwerty " + i).getBytes())).collect(toList());
        p.produceBatch(reqs);
        log.debug("adding batch took " + (System.currentTimeMillis() - start));

        log.debug("waiting for messages");
        start = System.currentTimeMillis();
        while (true) {
            if (rcvd.size() == 200) {
                break;
            }
            Thread.sleep(10);
            if ((System.currentTimeMillis() - start) > 3000) {
                log.debug("timed out waiting");
                break;
            }
        }
        log.debug("waiting for messages took " + (System.currentTimeMillis() - start));

        assertEquals(200, rcvd.size());



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