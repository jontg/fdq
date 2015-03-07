package com.relateiq.fdq;

import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by mbessler on 3/3/15.
 */
public class TopicStats {
    public final String name;
    public final long inserted;
    public final long acked;
    public final long acked_duration;
    public final long errored;
    public final long errored_duration;
    public final long timed_out;
    public final long popped;
    public final long running;
    public final Map<String, Collection<Integer>> assignments;
    public final long errored_duration_avg;
    public final long acked_duration_avg;


    public TopicStats(String name, Map<String, Collection<Integer>> assignments
            , long inserted
            , long acked
            , long acked_duration
            , long errored
            , long errored_duration
            , long timed_out
            , long popped
    ) {
        this.name = name;
        this.assignments = assignments;
        this.inserted = inserted;
        this.acked = acked;
        this.acked_duration = acked_duration;
        this.acked_duration_avg = acked == 0 ? 0 : acked_duration / acked;
        this.errored = errored;
        this.errored_duration = errored_duration;
        this.errored_duration_avg = errored == 0 ? 0 : errored_duration / errored;
        this.timed_out = timed_out;
        this.popped = popped;
        this.running = popped - acked;
    }

    @Override
    public String toString() {
        return "TopicStats{" +
                "name='" + name + '\'' +
                ", inserted=" + inserted +
                ", acked=" + acked +
                ", acked_duration=" + acked_duration +
                ", errored=" + errored +
                ", errored_duration=" + errored_duration +
                ", timed_out=" + timed_out +
                ", popped=" + popped +
                ", running=" + running +
                ", assignments=" + assignments +
                ", errored_duration_avg=" + errored_duration_avg +
                ", acked_duration_avg=" + acked_duration_avg +
                '}';
    }
}
