package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import org.junit.Test;

import java.util.stream.IntStream;

public class QueueTest {

    @Test
    public void tester() throws InterruptedException {

        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Queue q = new Queue(db);

        new Thread(() -> q.tail("asdf", e -> System.out.println("a " + e.shardKey + " " + new String(e.message)))).start();
        new Thread(() -> q.tail("asdf", e -> System.out.println("b " + e.shardKey + " " + new String(e.message)))).start();

        IntStream.range(0, 100).forEach(i -> q.enqueue("asdf", "" + i, ("qwerty " + i).getBytes()));

        Thread.sleep(1000);



//        db.run(new Function<Transaction, Void>() {
//            @Override
//            public Void apply(Transaction tr) {
//                for (KeyValue keyValue : tr.getRange(Range.startsWith(Tuple.from("asdf").pack()))) {
//                        System.out.println("hmm: " + new String(keyValue.getKey()) + " : " + new String(keyValue.getValue()));
//                }
//                return null;
//            }
//        });

//        Thread.sleep(1000);

    }

}