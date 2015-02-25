package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.FDB;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by mbessler on 2/23/15.
 */
public class FDQ {

    public static void main(String[] args) {

        switch (args[0]) {
            case "produce":
                produce();
                break;
            case "consume":
                consume();
                break;
        }
    }

    private static void consume() {
        String TOPIC = "testTopic";
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Consumer c = new Consumer(db);

        c.consume("testTopic", "" + (new Random()).nextLong(), e -> System.out.println(e.toString() + " " + new String(e.message)));

    }

    private static void produce() {
        String TOPIC = "testTopic";
        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Producer p = new Producer(db);

        Scanner in = new Scanner(System.in);
        try {
            BufferedReader b = new BufferedReader(new InputStreamReader(System.in));
            for(String s = null; (s = b.readLine()) != null;){
                String[] parts = s.split(" ", 2);
                String shardKey = parts[0];
                String message = parts[1];
                p.enqueue(TOPIC, shardKey, message.getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
