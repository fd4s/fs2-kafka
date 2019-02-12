# Functional Kafka Streams for Scala

This library is an FS2 wrapper around the official Java library for Kafka. It is not a rewrite of the Kafka client.

User focused documentation is available at https://ovotech.github.io/fs2-kafka  
The source of the documentation is in `docs/src/main`.

## Contributing

### Architecture

#### Understanding the consumer stream and the backpressure

There are 3 pieces at work in the consumer stream:

1. the poll scheduler -- responsible for throttling the poll requests via a poll queue (bounded with 1 element)
2. the consumer loop (`fs2.kafka.internal.KafkaConsumerActor`) -- handles both fetch requests issued by the stream as well as poll requests by the poll scheduler, using a state variable that accumulates fetch requests along with fetched kafka records. Records are only fetched when there is demand (backpressure).
3. the stream -- emits fetch requests on-demand (pull based)

These pieces are assembled by the `fs2.kafka.KafkaConsumer`, in the `consumerResource() method`. They all run concurrently in independant `Fiber`s.

There are a few important points to understand how the backpressure works.
1. FS2 Streams are pull based, meaning that there isn't a messaging system instructing up-stream to slow-down, but rather up-stream is asked to produce elements whenever down-stream is pulling. 
2. For obvious performance reasons we cannot go to Kafka everytime one kafka record is requested by down-stream. Rather we fetch records by batches and buffer up the un-consumed records.
3. This batch fetching is perform by the `KafkaConsumerActor` which keeps track of both fetch requests coming from downstream and fetched records in a `KafkaConsumerActor.State` variable.
4. To avoid excessive requests toward Kafka, the polling is throttled using a `pollInterval` (by the poll scheduler).
5. The `KafkaConsumerActor` "pauses" the Java client whenever no records are requested: see `KafkaConsumerActor.poll`:
```scala
def pollConsumer(state: State[F, K, V]): F[ConsumerRecords[K, V]] =
      withConsumer { consumer =>
        F.delay {
          val assigned = consumer.assignment.toSet
          val requested = state.fetches.keySetStrict
          val available = state.records.keySetStrict

          val resume = (requested intersect assigned) diff available
          val pause = assigned diff resume

          // without this the consumer would fetch indefinitely records which would potentially
          // accumulate faster (in the state) than they are consumed...
          consumer.pause(pause.asJava)
          consumer.resume(resume.asJava)
          consumer.poll(pollTimeout)
        }
      }
```

## Licence

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html). Refer to the [license file](https://github.com/ovotech/fs2-kafka/blob/master/license.txt).
