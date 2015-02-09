package com.relateiq.fdq;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import org.junit.Test;

public class QueueTest {

    @Test
    public void tester() throws InterruptedException {

        FDB fdb = FDB.selectAPIVersion(300);
        Database db = fdb.open();

        Queue q = new Queue(db);

        new Thread(() -> q.tail("asdf")).start();
        new Thread(() -> q.tail("asdf")).start();

        for (int i = 0; i < 100; i++) {
            q.enqueue("asdf", "qwerty " + i);
        }

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