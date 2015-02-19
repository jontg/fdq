package com.relateiq.fdq;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Created by mbessler on 2/10/15.
 */
public class Divvy {
    public static final Logger log = LoggerFactory.getLogger(Divvy.class);


    public static Multimap<String, Integer> addConsumer(Multimap<String, Integer> currentAssignments, String newConsumerId, int desiredTokenCount) {
        currentAssignments = ImmutableMultimap.copyOf(currentAssignments);

        int currentTokenCount = currentAssignments.asMap().values().stream().mapToInt(x -> x.size()).sum();
        if (currentAssignments.containsKey(newConsumerId) && currentTokenCount == desiredTokenCount) {
            return currentAssignments;
        }

        ImmutableMultimap.Builder<String, Integer> builder = ImmutableMultimap.builder();

        if (currentTokenCount != desiredTokenCount ){
            if (currentTokenCount != 0) {
                // nuke em all!
                throw new NotImplementedException();
            } else {
                for (int i = 0; i < desiredTokenCount; i++) {
                    builder.put(newConsumerId, i);
                }
                return builder.build();
            }
        }

        int currentConsumerCount = currentAssignments.keySet().size();

        int minTokens = desiredTokenCount / (currentConsumerCount + 1);
        int numToTakeFromEach = currentConsumerCount == 0 ? 0 : minTokens / currentConsumerCount;
        int remainder = currentConsumerCount == 0 ? 0 : minTokens % currentConsumerCount;

        // take min tokens for new consumer
        ImmutableSet.Builder<Integer> newConsumerTokensBuilder = ImmutableSet.builder();
        for (Map.Entry<String, Collection<Integer>> entry : currentAssignments.asMap().entrySet().stream()
                .sorted((a, b) -> b.getValue().size() - a.getValue().size()) // sorted by size descending
                .collect(toList())) {

            Collection<Integer> tokens = entry.getValue();
            int numToTake = numToTakeFromEach + (remainder > 0 ? 1 : 0);
            for (Integer token : Iterables.limit(tokens, numToTake)) {
                newConsumerTokensBuilder.add(token);
            }

            if (remainder > 0) {
                remainder--;
            }
        }


        ImmutableSet<Integer> newConsumerTokens = newConsumerTokensBuilder.build();
        for (Map.Entry<String, Collection<Integer>> entry : currentAssignments.asMap().entrySet()) {
            builder.putAll(entry.getKey(), Sets.difference(ImmutableSet.copyOf(entry.getValue()), newConsumerTokens));
        }

        builder.putAll(newConsumerId, newConsumerTokens);

        ImmutableMultimap<String, Integer> result = builder.build();
        int newTokenCount = result.asMap().values().stream().mapToInt(x -> x.size()).sum();

        assert newTokenCount == desiredTokenCount;

        return result;
    }
}
