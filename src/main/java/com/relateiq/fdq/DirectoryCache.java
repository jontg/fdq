package com.relateiq.fdq;

import com.foundationdb.Transaction;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;

import java.util.Arrays;

/**
 * Created by mbessler on 2/19/15.
 *
 * This class caches directory createOrOpen calls. It does so statically, so:
 *
 *  It is assumed you are not connecting to multiple FDB clusters.
 *
 */
public class DirectoryCache {

    public static final DirectoryLayer DIRECTORY_LAYER = DirectoryLayer.getDefault();

    public static DirectorySubspace mkdirp(Transaction tr, String... strings) {
        return DIRECTORY_LAYER.createOrOpen(tr, Arrays.asList(strings)).get();
    }

    public static void rmdir(Transaction tr, String... strings) {
        DIRECTORY_LAYER.removeIfExists(tr, Arrays.asList(strings)).get();
    }
}
