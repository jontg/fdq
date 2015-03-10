package com.relateiq.fdq.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relateiq.fdq.TopicStats;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;

public class FDQCliTest {

    @Test
    public void test() throws JsonProcessingException {
        System.out.println((new ObjectMapper()).writeValueAsString(new TopicStats("asdf", new HashMap<String, Collection<Integer>>(), 0, 0, 0, 0, 0, 0, 0)));
    }
}