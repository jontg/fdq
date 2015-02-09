# FDQ

## Goals

* Exactly once delivery
* Consistent
* Fast write throughput
* Simple elastic scaling
* Simple API
* Replayable

## Concepts

### Topic

A topic defines a specific queue that can be listened to.

### Message

A message is the unit of work for a given Topic

### Shard Key

A string that can be used to ensure all messages for a given shard key will be delivered to the same consumer.

Shard

To provide elastic scaling, the system will route messages written to a topic to a shard per consumer.

### Consumer

A process that listens to a topic. The system will route a subset of the messages using the Shard Key to a shard for this consumer.

### Cleanup

When a consumer stops heartbeating it will be considered a zombie. Any messages that remain in its shard will be re-routed to another consumer. This is accomplished by assigning additional shards to a consumer. Zombie shards will not receive additional messages and once drained it will be deleted.

## API

### Producer

```java
void push(String topic, byte[] message)
void push(String topic, String shardKey, byte[] message)
void pushMany(String topic, Iterable<Envelope> envelopes)
```

Pushes a message onto a topic. If provided, the shardKey will be mod-hashed to randomly route to different shards, otherwise messages will be randomly distributed to Consumers. pushMany allows for transactionally pushing many messages at once.

```java
void replay(String topic, long start)
void replay(String topic, long start, long end)
```

### Supervisor

```java
void pause(String topic)
void play(String topic)
```

These control whether shard and processing for a topic are activated.

```java
void rebalance(String topic)
```

This will pause a topic and re-route messages that are in shard queues. This is useful to  redistribute a backlog to new Consumers. (Should this be automatic when adding a Consumer?)

### Consumer

```java
class Envelope {
  long insertionTime; // client time (use NTP!)
  String shardKey;
  byte[] message;
}
```

```java
class ConsumerInstance {
  final String id;
  final boolean isAcking; // is the consumer using the acking mechanism to ensure reliability
  int batchSize;          // the # of messages read from the shard at a time

}
```

An instance of a Consumer, maintains client configuration state and provides lifecycle methods.

```java
static ConsumerInstance tail(String topic, Consumer<Envelope> handler)
```

Create a ConsumerInstance to tail new messages for a topic. (future: other configuration)

```java
void shutdown();
```

Shutting down a consumer will mark itself as a zombie. This Consumer can no longer be used.

## How it works

Pushing a message writes a message to the appropriate shard queue given the shardKey for a given topic, along with the epoch millis of insertion time and a random string to prevent write conflicts. Further all messages are written to the All "shard" for replayability purposes.

### Pushing a message

1. Ensure the topic exists. By default, pushing to a non-existant topic will create the topic.
1. Find out how many shards there currently are.
1. Use consistent mod-hashing on the supplied shardKey to figure out which shard a message should be written to (shardids are 0-indexed)
1. Write the message to that shard, as well as the "all" "shard" (for replayability)
1. Increment appropriate stats

### Adding a consumer

1. Create a new consumer instance by calling tail on the topic
1. Adjust the # of shards 

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
