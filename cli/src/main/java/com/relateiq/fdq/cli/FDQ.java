package com.relateiq.fdq.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.relateiq.fdq.Consumer;
import com.relateiq.fdq.Helpers;
import com.relateiq.fdq.MessageRequest;
import com.relateiq.fdq.Producer;
import com.relateiq.fdq.TopicConfig;
import com.relateiq.fdq.TopicManager;
import com.relateiq.fdq.TopicProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * Created by mbessler on 2/23/15.
 */
public class FDQ {
    public static final Logger log = LoggerFactory.getLogger(FDQ.class);
    private static final Random random = new Random();

    public static void main(String[] args) throws JsonProcessingException {

        switch (args[0]) {
            case "produce":
                produce();
                break;
            case "produce-random":
                produceRandom();
                break;
            case "consume":
                consume();
                break;
            case "status":
                status();
                break;
            case "nuke":
                nuke();
                break;
        }
    }

    private static void nuke() {
        String TOPIC = "testTopic";
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();
        TopicManager manager = new TopicManager(db, Helpers.createTopicConfig(db, TOPIC));

        manager.nuke();
    }

    private static void status() throws JsonProcessingException {
        String TOPIC = "testTopic";
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();
        TopicManager manager = new TopicManager(db, Helpers.createTopicConfig(db, TOPIC));
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());

        System.out.println(mapper.writeValueAsString(manager.stats()));
    }

    private static void produceRandom() {
        String TOPIC = "testTopic";
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        TopicProducer p = new Producer(db).createProducer(TOPIC);

        while (true) {
            List<MessageRequest> reqs = IntStream.range(0, 1000).mapToObj(i -> new MessageRequest("" + i, ("qwerty " + i).getBytes())).collect(toList());
            p.produceBatch(reqs);

//            p.produce(Long.toString(random.nextLong()), Long.toString(random.nextLong()).getBytes());
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                break;
//            }

        }
    }

    private static void consume() {
        String TOPIC = "testTopic";
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Consumer c = new Consumer(db);

        c.consume("testTopic", "" + (new Random()).nextLong(), e -> {
            try {
                Thread.sleep(new Random().nextInt(6000));
            } catch (InterruptedException e1) {
            }
            System.out.println(e.toString() + " " + new String(e.message));});

    }

    private static void produce() {
        String TOPIC = "testTopic";
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        TopicProducer p = new Producer(db).createProducer(TOPIC);

        Scanner in = new Scanner(System.in);
        try {
            BufferedReader b = new BufferedReader(new InputStreamReader(System.in));
            for(String s = null; (s = b.readLine()) != null;){
                String[] parts = s.split(" ", 2);
                String shardKey = parts[0];
                String message = parts[1];
                p.produce(shardKey, message.getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
