package com.relateiq.fdq;

import com.foundationdb.Transaction;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.Tuple;

import java.util.Map;

/**
 * Created by mbessler on 2/23/15.
 */
public class TopicDirectories {

    public final DirectorySubspace assignments;
    public final DirectorySubspace heartbeats;
    public final Map<Integer, DirectorySubspace> shardMetrics;
    public final Map<Integer, DirectorySubspace> shardData;


    public TopicDirectories(DirectorySubspace assignments, DirectorySubspace heartbeats, Map<Integer, DirectorySubspace> shardMetrics, Map<Integer, DirectorySubspace> shardData) {
        this.assignments = assignments;
        this.heartbeats = heartbeats;
        this.shardMetrics = shardMetrics;
        this.shardData = shardData;
    }

    public byte[] getTopicShardWatchKey(Integer shardIndex) {
        return shardMetrics.get(shardIndex).pack(Tuple.from("inserted"));
    }

    public byte[] getTopicAssignmentsKey(Integer shardIndex) {
        return assignments.pack(shardIndex);
    }


}
