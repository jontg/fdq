package com.relateiq.fdq;

/**
 * Created by mbessler on 3/3/15.
 */
public class TopicStats {
    public final String name;
    public final long inserted;
    public final long acked;
    public final long errored;
    public final long popped;

    public TopicStats(String name, long inserted, long acked, long errored, long popped) {
        this.name = name;
        this.inserted = inserted;
        this.acked = acked;
        this.errored = errored;
        this.popped = popped;
    }

    @Override
    public String toString() {
        return "{" +
                "\"name\" : \"" + name + "\"" +
                ", \"inserted\" : \"" + inserted + "\"" +
                ", \"acked\" : \"" + acked + "\"" +
                ", \"errored\" : \"" + errored + "\"" +
                ", \"popped\" : \"" + popped + "\"" +
                '}';
    }
}
