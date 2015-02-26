# FDQ

## Goals

* Exactly once delivery
* Consistent
* Fast write throughput
* Simple elastic scaling
* Simple API
* Replayable
* Batteries included (stats, operational admin functions, etc.)

## Concepts

### Topic

A topic defines a specific queue that can be listened to.

### Message

A message is the unit of work for a given Topic

### Shard Key

A string that can be used to ensure all messages for a given shard key will be delivered to the same consumer.

### Shard

To provide elastic scaling, the system will consistently route messages written to a topic to a shard

### Consumer

A process that listens to a topic. The shards will be distributed evenly amongst the live consumers.

## API

### Producer

```java
class MessageRequest {
    final String shardKey;
    final byte[] message;
}
```
Used by produceBatch

```java
void produce(String topic, String shardKey, byte[] message);
void produceBatch(String topic, Collection<MessageRequest> requests);
```

Produce a message for a topic. The shardKey will be consistently mod-hashed to choose which shard to route to. produceBatch allows for transactionally pushing many messages at once, greatly improving throughput.


### Consumer

```java
class Envelope {
  long insertionTime; // client time (use NTP!)
  String shardKey;
  byte[] message;
  int shardIndex;
  int executorIndex;
}
```

An instance of a Consumer, maintains client configuration state and provides lifecycle methods.

```java
void consume(final String topic, String consumerName, java.util.function.Consumer<Envelope> consumer);
```

Create a Consumer to tail a topic. (future: other configuration)

## How it works

Pushing a message writes a message to the appropriate shard queue given the `shardKey` for a given topic, along with the epoch millis of insertion time and a random string to prevent write conflicts. Further all messages are written to the All "shard" for replayability purposes.

### Pushing a message

1. Ensure the topic exists. By default, pushing to a non-existant topic will create the topic.
1. Find out how many shards there currently are.
1. Use consistent mod-hashing on the supplied `shardKey` to figure out which shard a message should be written to (shardids are 0-indexed)
1. Write the message to that shard, as well as the "all" "shard" (for replayability)
1. Increment appropriate stats

### Adding a consumer

1. Create a new consumer by calling `consume(...)` on a Consumer object
1. Update assignments to give some shards to this new Consumer

### Removing a consumer

### Replaying messages

### Config Internals

```

Which topics are there?

For a topic, how

fdq/config/topics/[topic]

fdq/config/topics/[topic]/consumers/[consumerid] -> <serialized list of shard ids>
fdq/config/topics/[topic]/shards/[shardid]


```

### Topic Internals

```
fdq/topic/all/stats/inserted
```
A count of the # of inserted messages

```
fdq/topic/all/stats/routed
```
A count of the # of routed messages (those that have been put in a shard)

```
fdq/topic/all/stats/emitted
```
A count of the # of emitted messages (those that a consumer has emitted)

```
fdq/topic/all/stats/acked
```
A count of the # of acked messages (those that a consumer has acknowledged as being complete)

```
fdq/topic/all/data/[insertiontime]:[random]:[shardKey] -> message
```
The actual content (messages) of the topic.


### Shard Internals

```
fdq/topic/shards/[shardid]/stats/*
fdq/topic/shards/[shardid]/data/
```

Same stats as "all", but per shard.
