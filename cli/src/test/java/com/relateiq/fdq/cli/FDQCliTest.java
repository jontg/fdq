package com.relateiq.fdq.cli;

import com.alibaba.fastjson.JSON;
import com.relateiq.fdq.TopicStats;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;

public class FDQCliTest {

    @Test
    public void test() {
        System.out.println(JSON.toJSONString(new TopicStats("asdf", new HashMap<String, Collection<Integer>>(), 0, 0, 0, 0, 0, 0, 0)));
    }
}