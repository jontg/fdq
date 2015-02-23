package com.relateiq.fdq;

import com.foundationdb.Transaction;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mbessler on 2/19/15.
 *
 * This class caches directory createOrOpen calls. It does so statically, so:
 *
 *  It is assumed you are not connecting to multiple FDB clusters.
 *
 */
public class DirectoryCache {

    private static final ConcurrentHashMap<List<String>, DirectorySubspace> directories = new ConcurrentHashMap<>();

    public static DirectorySubspace directory(Transaction tr, String... strings) {
        List<String> key = Arrays.asList(strings);
        DirectorySubspace result = directories.get(key);
        if (result != null) {
            return result;
        }
        result = DirectoryLayer.getDefault().createOrOpen(tr, key).get();
        directories.putIfAbsent(key, result);
        return result;
    }
}
