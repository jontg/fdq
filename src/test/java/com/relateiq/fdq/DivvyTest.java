package com.relateiq.fdq;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import static com.relateiq.fdq.Divvy.addConsumer;
import static com.relateiq.fdq.Divvy.log;
import static com.relateiq.fdq.Divvy.removeConsumer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DivvyTest {

    @Test
    public void divvyTests() {
        int tokenCount = 32;

        Multimap<String, Integer> assignments = HashMultimap.create();
        assignments = addConsumer(assignments, "a", tokenCount);
        log.info(assignments.toString());

        assertEquals(32, assignments.get("a").size());

        assignments = addConsumer(assignments, "b", tokenCount);

        log.info(assignments.toString());

        assertEquals(16, assignments.get("a").size());
        assertEquals(16, assignments.get("b").size());

        assignments = addConsumer(assignments, "c", tokenCount);
        log.info(assignments.toString());

        assertEquals(11, assignments.get("a").size());
        assertEquals(11, assignments.get("b").size());
        assertEquals(10, assignments.get("c").size());

        assignments = addConsumer(assignments, "d", tokenCount);
        log.info(assignments.toString());

        assertEquals(8, assignments.get("a").size());
        assertEquals(8, assignments.get("b").size());
        assertEquals(8, assignments.get("c").size());
        assertEquals(8, assignments.get("d").size());

        assignments = addConsumer(assignments, "e", tokenCount);
        log.info(assignments.toString());

        for (Map.Entry<String, Collection<Integer>> entry : assignments.asMap().entrySet()) {
            assertTrue(entry.getValue().size() >= 6);
            assertTrue(entry.getValue().size() <= 7);
        }

        assignments = addConsumer(assignments, "f", tokenCount);
        log.info(assignments.toString());

        for (Map.Entry<String, Collection<Integer>> entry : assignments.asMap().entrySet()) {
            assertTrue(entry.getValue().size() >= 5);
            assertTrue(entry.getValue().size() <= 6);
        }

        assignments = removeConsumer(assignments, "f", tokenCount);
        log.info(assignments.toString());

        for (Map.Entry<String, Collection<Integer>> entry : assignments.asMap().entrySet()) {
            assertTrue(entry.getValue().size() >= 6);
            assertTrue(entry.getValue().size() <= 7);
        }

        assignments = removeConsumer(assignments, "e", tokenCount);
        log.info(assignments.toString());

        assertEquals(8, assignments.get("a").size());
        assertEquals(8, assignments.get("b").size());
        assertEquals(8, assignments.get("c").size());
        assertEquals(8, assignments.get("d").size());

        assignments = removeConsumer(assignments, "d", tokenCount);
        log.info(assignments.toString());

        assertEquals(11, assignments.get("a").size());
        assertEquals(11, assignments.get("b").size());
        assertEquals(10, assignments.get("c").size());

        assignments = removeConsumer(assignments, "c", tokenCount);
        log.info(assignments.toString());

        assertEquals(16, assignments.get("a").size());
        assertEquals(16, assignments.get("b").size());

        assignments = removeConsumer(assignments, "b", tokenCount);
        log.info(assignments.toString());

        assertEquals(32, assignments.get("a").size());

        assignments = removeConsumer(assignments, "a", tokenCount);

        log.info(assignments.toString());
        assertEquals(0, assignments.size());



    }

}