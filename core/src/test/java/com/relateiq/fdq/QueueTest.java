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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueueTest {
    public static final Logger log = LoggerFactory.getLogger(QueueTest.class);
    public static final Random RANDOM = new Random();

    @Test
    public void tester() throws InterruptedException {
        String TOPIC = "" + RANDOM.nextLong();
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Consumer c = new Consumer(db);
        TopicProducer p = new Producer(db).createProducer(TOPIC);
        TopicManager m = new TopicManager(db, p.topicConfig);

        Multiset<String> rcvd = ConcurrentHashMultiset.create();

        log.debug("creating consumers");
        c.createConsumer(TOPIC, "a", e -> rcvd.add(new String(e.message)));
        c.createConsumer(TOPIC, "b", e -> rcvd.add(new String(e.message)));
        c.createConsumer(TOPIC, "c", e -> rcvd.add(new String(e.message)));

        log.debug("adding serially");
        long start = System.currentTimeMillis();
        int COUNT1 = 100;
        int COUNT = COUNT1*2;

        IntStream.range(0, COUNT1).forEach(i -> p.produce("" + i, ("qwerty " + i).getBytes()));
        log.debug("adding serially took " + (System.currentTimeMillis() - start));

        log.debug("adding batch");
        start = System.currentTimeMillis();
        List<MessageRequest> reqs = IntStream.range(0, COUNT1).mapToObj(i -> new MessageRequest("" + i, ("qwerty " + i).getBytes())).collect(toList());
        p.produceBatch(reqs);
        log.debug("adding batch took " + (System.currentTimeMillis() - start));

        log.debug("waiting for messages");
        start = System.currentTimeMillis();
        while (true) {
            if (rcvd.size() == COUNT && m.runningCount() == 0) {
                break;
            }
            Thread.sleep(10);
            if ((System.currentTimeMillis() - start) > 3000) {
                log.debug("timed out waiting");
                break;
            }
        }
        log.debug("waiting for messages took " + (System.currentTimeMillis() - start));

        assertEquals(0, m.runningCount());
        assertEquals(COUNT, rcvd.size());

        log.debug(m.stats().toString());

        m.nuke();
    }

    @Test
    public void testDeactivate() throws InterruptedException {
        String TOPIC = "" + RANDOM.nextLong();
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Consumer c = new Consumer(db);
        TopicProducer p = new Producer(db).createProducer(TOPIC);
        TopicManager m = new TopicManager(db, p.topicConfig);

        log.debug("creating consumers");
        c.createConsumer(TOPIC, "a", e -> e.toString());
        c.createConsumer(TOPIC, "b", e -> e.toString());
        c.createConsumer(TOPIC, "c", e -> e.toString());

        log.debug("deactivating");
        m.deactivate();
        assertFalse(m.isActivated());

        log.debug("adding tuples");
        int COUNT1 = 10;
        List<MessageRequest> reqs = IntStream.range(0, COUNT1).mapToObj(i -> new MessageRequest("" + i, ("qwerty " + i).getBytes())).collect(toList());
        p.produceBatch(reqs);

        assertEquals(0, m.stats().acked);


        log.debug("activating");
        m.activate();
        assertTrue(m.isActivated());

        log.debug("waiting for messages");
        long start = System.currentTimeMillis();
        while (true) {
            if (m.stats().acked == COUNT1) {
                break;
            }
            Thread.sleep(10);
            if ((System.currentTimeMillis() - start) > TopicConsumer.HEARTBEAT_MILLIS * 2) {
                log.debug("timed out waiting");
                break;
            }
        }
        log.debug("waiting for messages took " + (System.currentTimeMillis() - start));

        assertEquals(0, m.runningCount());
        assertEquals(COUNT1, m.stats().acked);
        log.debug(m.stats().toString());

        m.nuke();
    }

    @Test
    public void testErrors() throws InterruptedException {
        String TOPIC = "" + RANDOM.nextLong();
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Consumer c = new Consumer(db);
        TopicProducer p = new Producer(db).createProducer(TOPIC);
        TopicManager m = new TopicManager(db, p.topicConfig);

        log.debug("creating consumers");
        c.createConsumer(TOPIC, "a", e -> {
            throw new RuntimeException("asdf");
        });

        int COUNT = 5;
        List<MessageRequest> reqs = IntStream.range(0, COUNT).mapToObj(i -> new MessageRequest("" + i, ("qwerty " + i).getBytes())).collect(toList());
        p.produceBatch(reqs);

        log.debug("waiting for messages");
        long start = System.currentTimeMillis();
        while (true) {
            if (m.stats().errored == COUNT && m.runningCount() == 0) {
                break;
            }
            Thread.sleep(10);
            if ((System.currentTimeMillis() - start) > 3000) {
                log.debug("timed out waiting");
                break;
            }
        }
        log.debug("waiting for messages took " + (System.currentTimeMillis() - start));

        assertEquals(0, m.runningCount());
        assertEquals(COUNT, m.stats().errored);

        log.debug(m.stats().toString());

        m.nuke();
    }

}