package fs2.kafka

import cats.data.NonEmptySet
import cats.effect.concurrent.Ref
import cats.effect.IO
import cats.implicits._
import fs2.concurrent.Queue
import fs2.kafka.internal.converters.collection._
import fs2.Stream
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.TopicPartition
import scala.collection.immutable.SortedSet
import scala.concurrent.duration._

final class KafkaConsumerSpec extends BaseKafkaSpec {

  type Consumer = KafkaConsumer[IO, String, String]

  type ConsumerStream = Stream[IO, CommittableConsumerRecord[IO, String, String]]

  describe("KafkaConsumer#stream") {
    it("should consume all records with subscribe") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 5).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val consumed =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.subscribeTo(topic))
            .evalTap(consumer => IO(consumer.toString should startWith("KafkaConsumer$")).void)
            .evalMap(IO.sleep(3.seconds).as(_)) // sleep a bit to trigger potential race condition with _.stream
            .flatMap(_.stream)
            .map(committable => committable.record.key -> committable.record.value)
            .interruptAfter(10.seconds) // wait some time to catch potentially duplicated records
            .compile
            .toVector
            .unsafeRunSync()

        consumed should contain theSameElementsAs produced
      }
    }

    it("should consume all records with subscribing for several consumers") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 5).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val consumed =
          consumerStream[IO]
            .using(consumerSettings[IO](config).withGroupId("test"))
            .evalTap(_.subscribeTo(topic))
            .evalMap(IO.sleep(3.seconds).as(_)) // sleep a bit to trigger potential race condition with _.stream
            .flatMap(_.stream)
            .map(committable => committable.record.key -> committable.record.value)
            .interruptAfter(10.seconds) // wait some time to catch potentially duplicated records

        val res = fs2
          .Stream(
            consumed,
            consumed
          )
          .parJoinUnbounded
          .compile
          .toVector
          .unsafeRunSync()

        res should contain theSameElementsAs produced

      }
    }

    it("should consume records with assign by partitions") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 5).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val partitions = NonEmptySet.fromSetUnsafe(SortedSet(0, 1, 2))

        val consumed =
          consumerStream[IO]
            .using(consumerSettings[IO](config).withGroupId("test2"))
            .evalTap(_.assign(topic, partitions))
            .evalTap(consumer => IO(consumer.toString should startWith("KafkaConsumer$")).void)
            .evalMap(IO.sleep(3.seconds).as(_)) // sleep a bit to trigger potential race condition with _.stream
            .flatMap(_.stream)
            .map(committable => committable.record.key -> committable.record.value)
            .interruptAfter(10.seconds)

        val res =
          consumed.compile.toVector
            .unsafeRunSync()

        res should contain theSameElementsAs produced
      }
    }

    it("should consume all records with assign without partitions") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 5).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val consumed =
          consumerStream[IO]
            .using(consumerSettings[IO](config).withGroupId("test"))
            .evalTap(_.assign(topic))
            .evalMap(IO.sleep(3.seconds).as(_)) // sleep a bit to trigger potential race condition with _.stream
            .flatMap(_.stream)
            .map(committable => committable.record.key -> committable.record.value)
            .interruptAfter(10.seconds)

        val res =
          consumed.compile.toVector
            .unsafeRunSync()

        res should contain theSameElementsAs produced
      }
    }

    it("should consume all records to several consumers with assign") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 5).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val consumed =
          consumerStream[IO]
            .using(consumerSettings[IO](config).withGroupId("test2"))
            .evalTap(_.assign(topic))
            .evalMap(IO.sleep(3.seconds).as(_)) // sleep a bit to trigger potential race condition with _.stream
            .flatMap(_.stream)
            .map(committable => committable.record.key -> committable.record.value)
            .interruptAfter(10.seconds)

        val res = fs2
          .Stream(
            consumed,
            consumed
          )
          .parJoinUnbounded
          .compile
          .toVector
          .unsafeRunSync()

        res should contain theSameElementsAs produced ++ produced
      }
    }

    it("should correctly get partitions from topic via partitionsFor") {
      withKafka { (config, topic) =>
        val partitions = List(0, 1, 2)

        createCustomTopic(topic, partitions = partitions.size)

        val info =
          consumerStream[IO]
            .using(consumerSettings[IO](config))
            .evalMap(_.partitionsFor(topic))

        val res =
          info.compile.lastOrError
            .unsafeRunSync()

        res.map(_.partition()) should contain theSameElementsAs partitions
        res.map(_.topic()).toSet should contain theSameElementsAs Set(topic)
      }
    }

    it("should fail when to get partitions from topic via partitionsFor with a small timeout") {
      withKafka { (config, topic) =>
        val partitions = List(0, 1, 2)

        createCustomTopic(topic, partitions = partitions.size)

        val info =
          consumerStream[IO]
            .using(consumerSettings[IO](config))
            .evalTap(_.partitionsFor(topic, 1.nanos))

        val res =
          info.compile.lastOrError.attempt
            .unsafeRunSync()

        res.left.toOption match {
          case Some(e) => e shouldBe a[TimeoutException]
          case _       => fail("No exception was rised!")
        }
      }
    }

    it("should get metrics") {
      withKafka { (config, topic) =>
        val partitions = List(0, 1, 2)

        createCustomTopic(topic, partitions = partitions.size)

        val info =
          consumerStream[IO]
            .using(consumerSettings[IO](config))
            .evalMap(_.metrics)

        val res =
          info
            .take(1)
            .compile
            .lastOrError
            .unsafeRunSync()

        assert(res.nonEmpty)
      }
    }

    it("should correctly unsubscribe") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 1).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val cons = consumerStream[IO]
          .using(consumerSettings[IO](config).withGroupId("test"))
          .evalTap(_.subscribeTo(topic))

        val topicStream = (for {
          cntRef <- Stream.eval(Ref.of[IO, Int](0))
          unsubscribed <- Stream.eval(Ref.of[IO, Boolean](false))
          partitions <- Stream.eval(Ref.of[IO, Set[TopicPartition]](Set.empty[TopicPartition]))

          consumer1 <- cons
          consumer2 <- cons

          _ <- Stream(
            consumer1.stream.evalTap(_ => cntRef.update(_ + 1)),
            consumer2.stream.concurrently(
              consumer2.assignmentStream.evalTap(
                assignedTopicPartitions => partitions.set(assignedTopicPartitions)
              )
            )
          ).parJoinUnbounded

          cntValue <- Stream.eval(cntRef.get)
          unsubscribedValue <- Stream.eval(unsubscribed.get)
          _ <- Stream.eval(
            if (cntValue >= 3 && !unsubscribedValue) //wait for some processed elements from first consumer
              unsubscribed.set(true) >> consumer1.unsubscribe // unsubscribe
            else IO.unit
          )
          _ <- Stream.eval(IO { publishToKafka(topic, produced) }) // publish some elements to topic

          partitionsValue <- Stream.eval(partitions.get)
        } yield (partitionsValue)).interruptAfter(10.seconds)

        val res = topicStream.compile.toVector
          .unsafeRunSync()

        res.last.size shouldBe 3 // in last message should be all partitions
      }
    }

    it("should handle rebalance") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced1 = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        val produced2 = (100 until 200).map(n => s"key-$n" -> s"value->$n")
        val producedTotal = produced1.size.toLong + produced2.size.toLong

        val consumer = (queue: Queue[IO, CommittableConsumerRecord[IO, String, String]]) =>
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.subscribeTo(topic))
            .evalMap(_.stream.evalMap(queue.enqueue1).compile.drain.start.void)

        (for {
          queue <- Stream.eval(Queue.unbounded[IO, CommittableConsumerRecord[IO, String, String]])
          ref <- Stream.eval(Ref.of[IO, Map[String, Int]](Map.empty))
          _ <- consumer(queue)
          _ <- Stream.eval(IO.sleep(5.seconds))
          _ <- Stream.eval(IO(publishToKafka(topic, produced1)))
          _ <- consumer(queue)
          _ <- Stream.eval(IO.sleep(5.seconds))
          _ <- Stream.eval(IO(publishToKafka(topic, produced2)))
          _ <- Stream.eval {
            queue.dequeue
              .evalMap { committable =>
                ref.modify { counts =>
                  val key = committable.record.key
                  val newCounts = counts.updated(key, counts.getOrElse(key, 0) + 1)
                  (newCounts, newCounts)
                }
              }
              .takeWhile(_.size < 200)
              .compile
              .drain
          }
          keys <- Stream.eval(ref.get)
          _ <- Stream.eval(IO(assert {
            keys.size.toLong == producedTotal && {
              keys == (0 until 200).map { n =>
                s"key-$n" -> (if (n < 100) 2 else 1)
              }.toMap
            }
          }))
        } yield ()).compile.drain.unsafeRunSync()
      }
    }

    it("should read from the given offset") {
      withKafka {
        seekTest(numRecords = 100, readOffset = 90)
      }
    }

    it("should fail to read from a negative offset") {
      withKafka {
        an[IllegalArgumentException] should be thrownBy seekTest(
          numRecords = 100,
          readOffset = -1
        )(_, _)
      }
    }

    it("should fail to read from a partition not assigned to this consumer") {
      withKafka {
        an[IllegalStateException] should be thrownBy seekTest(
          numRecords = 100,
          readOffset = 90,
          partition = Some(123)
        )(_, _)
      }
    }

    it("should commit the last processed offsets") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val committed =
          Stream(consumerSettings[IO](config))
            .flatMap(consumerStream[IO].using)
            .evalTap(_.subscribe(topic.r))
            .flatMap { consumer =>
              consumer.stream
                .take(produced.size.toLong)
                .map(_.offset)
                .fold(CommittableOffsetBatch.empty[IO])(_ updated _)
                .evalMap(batch => batch.commit.as(batch.offsets))
            }
            .compile
            .lastOrError
            .unsafeRunSync()

        assert {
          committed.values.toList.foldMap(_.offset) == produced.size.toLong &&
          withKafkaConsumer(consumerProperties(config)) { consumer =>
            committed.values.toList.foldMap(_.offset)

            committed.foldLeft(true) {
              case (result, (topicPartition, offsetAndMetadata)) =>
                result &&
                  consumer
                    .committed(Set(topicPartition).asJava)
                    .asScala
                    .get(topicPartition)
                    .contains(offsetAndMetadata)
            }
          }
        }
      }
    }

    it("should interrupt the stream when cancelled") {
      withKafka { (config, topic) =>
        val consumed =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.subscribeTo(topic))
            .evalTap(_.fiber.cancel)
            .flatTap(_.stream)
            .evalTap(_.fiber.join)
            .compile
            .toVector
            .unsafeRunSync()

        assert(consumed.isEmpty)
      }
    }

    it("should fail with an error if not subscribed or assigned") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)

        val consumed =
          consumerStream[IO]
            .using(consumerSettings(config))
            .flatMap(_.stream)
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert(consumed.left.toOption.map(_.toString).contains(NotSubscribedException().toString))
      }
    }

    it("should fail with an error if subscribe is invalid") {
      withKafka { (config, _) =>
        val subscribeName =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalMap(_.subscribeTo(""))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert(subscribeName.isLeft)

        val subscribeRegex =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.subscribeTo("topic"))
            .evalMap(_.subscribe("".r))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert(subscribeRegex.isLeft)
      }
    }

    it("should fail with an error if assign is invalid") {
      withKafka { (config, _) =>
        val assignEmptyName =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalMap(_.assign("", NonEmptySet.fromSetUnsafe(SortedSet(0))))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert {
          assignEmptyName.left.toOption
            .map(_.toString)
            .contains {
              "java.lang.IllegalArgumentException: Topic partitions to assign to cannot have null or empty topic"
            }
        }
      }
    }

    it("an error should occur if subscribe and assign at the same time") {
      withKafka { (config, topic) =>
        val assignWithSubscribeName =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.subscribeTo(topic))
            .evalMap(_.assign(topic, NonEmptySet.fromSetUnsafe(SortedSet(0))))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert {
          assignWithSubscribeName.left.toOption
            .map(_.toString)
            .contains {
              "java.lang.IllegalStateException: Subscription to topics, partitions and pattern are mutually exclusive"
            }
        }

        val subscribeWithAssignWithName =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.assign(topic, NonEmptySet.fromSetUnsafe(SortedSet(0))))
            .evalMap(_.subscribeTo(topic))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert {
          subscribeWithAssignWithName.left.toOption
            .map(_.toString)
            .contains {
              "java.lang.IllegalStateException: Subscription to topics, partitions and pattern are mutually exclusive"
            }
        }

      }
    }

    it("should propagate consumer errors to stream") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)

        val consumed =
          consumerStream[IO]
            .using {
              consumerSettings[IO](config)
                .withAutoOffsetReset(AutoOffsetReset.None)
            }
            .evalTap(_.subscribeTo(topic))
            .flatMap(_.stream)
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        consumed.left.toOption match {
          case Some(_: NoOffsetForPartitionException) => succeed
          case Some(cause)                            => fail("Unexpected exception", cause)
          case None                                   => fail(s"Unexpected result [$consumed]")
        }
      }
    }

    it("should be able to work with offsets") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 1)
        val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val topicPartition = new TopicPartition(topic, 0)
        val topicPartitions = Set(topicPartition)
        val timeout = 10.seconds

        consumerStream[IO]
          .using(consumerSettings(config))
          .evalTap(_.subscribeTo(topic))
          .flatTap { consumer =>
            consumer.stream
              .take(produced.size.toLong)
              .map(_.offset)
              .chunks
              .evalMap(CommittableOffsetBatch.fromFoldable(_).commit)
          }
          .evalTap { consumer =>
            for {
              start <- consumer.beginningOffsets(topicPartitions)
              startTimeout <- consumer.beginningOffsets(topicPartitions, timeout)
              _ <- IO(assert(start == startTimeout && start == Map(topicPartition -> 0L)))
            } yield ()
          }
          .evalTap { consumer =>
            for {
              end <- consumer.endOffsets(topicPartitions)
              endTimeout <- consumer.endOffsets(topicPartitions, timeout)
              _ <- IO(
                assert(end == endTimeout && end == Map(topicPartition -> produced.size.toLong))
              )
            } yield ()
          }
          .evalTap { consumer =>
            for {
              assigned <- consumer.assignment
              _ <- IO(assert(assigned.nonEmpty))
              _ <- consumer.seekToBeginning(assigned)
              start <- assigned.toList.parTraverse(consumer.position)
              _ <- IO(start.forall(_ === 0))
              _ <- consumer.seekToEnd(assigned)
              end <- assigned.toList.parTraverse(consumer.position(_, 10.seconds))
              _ <- IO(end.sum === produced.size)
              _ <- consumer.seekToBeginning
              start <- assigned.toList.parTraverse(consumer.position)
              _ <- IO(start.forall(_ === 0))
              _ <- consumer.seekToEnd
              end <- assigned.toList.parTraverse(consumer.position(_, 10.seconds))
              _ <- IO(end.sum === produced.size)
            } yield ()
          }
          .compile
          .drain
          .unsafeRunSync()
      }
    }

    def seekTest(numRecords: Long, readOffset: Long, partition: Option[Int] = None)(
      config: EmbeddedKafkaConfig,
      topic: String
    ) = {
      createCustomTopic(topic)

      val produced = (0L until numRecords).map(n => s"key-$n" -> s"value->$n")
      publishToKafka(topic, produced)

      val consumed =
        consumerStream[IO]
          .using(consumerSettings(config))
          .flatMap { consumer =>
            val validSeekParams =
              consumer.stream
                .take(Math.max(readOffset, 1))
                .map(_.offset)
                .compile
                .toList
                .map(_.last)
                .map(co => (co.topicPartition, co.offsetAndMetadata.offset()))

            val seekParams =
              validSeekParams.map {
                case (topicPartition, offset) =>
                  val p = partition.map(new TopicPartition(topic, _)).getOrElse(topicPartition)
                  val o = Math.min(readOffset, offset)

                  (p, o)
              }

            val setOffset =
              seekParams.flatMap { case (tp, o) => consumer.seek(tp, o) }

            val consume = consumer.stream.take(numRecords - readOffset)

            Stream.eval(consumer.subscribeTo(topic)).drain ++
              (Stream.eval_(setOffset) ++ consume)
                .map(_.record)
                .map(record => record.key -> record.value)
          }
          .compile
          .toVector
          .unsafeRunSync()

      consumed should contain theSameElementsAs produced.drop(readOffset.toInt)
    }
  }

  describe("KafkaConsumer#assignmentStream") {
    it("should stream assignment updates to listeners") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)

        val consumer =
          for {
            queue <- Stream.eval(Queue.noneTerminated[IO, SortedSet[TopicPartition]])
            _ <- consumerStream[IO]
              .using(consumerSettings(config))
              .evalTap(_.subscribeTo(topic))
              .evalMap { consumer =>
                consumer.assignmentStream
                  .concurrently(consumer.stream)
                  .evalMap(as => queue.enqueue1(Some(as)))
                  .compile
                  .drain
                  .start
                  .void
              }
          } yield {
            queue
          }

        (for {
          queue1 <- consumer
          _ <- Stream.eval(IO.sleep(5.seconds))
          queue2 <- consumer
          _ <- Stream.eval(IO.sleep(5.seconds))
          _ <- Stream.eval(queue1.enqueue1(None))
          _ <- Stream.eval(queue2.enqueue1(None))
          consumer1Updates <- Stream.eval(queue1.dequeue.compile.toList)
          consumer2Updates <- Stream.eval(queue2.dequeue.compile.toList)
          _ <- Stream.eval(IO(assert {
            // Startup assignments (zero), initial assignments (all topics),
            // revoke all on 2nd joining (zero), assign rebalanced set (< 3)
            consumer1Updates.length == 4 &&
            consumer1Updates.head.isEmpty &&
            consumer1Updates(1).size == 3 &&
            consumer1Updates(2).isEmpty &&
            consumer1Updates(3).size < 3 &&
            // Startup assignments (zero), initial assignments (< 3)
            consumer2Updates.length == 2
            consumer2Updates.head.isEmpty &&
            consumer2Updates(1).size < 3 &&
            (consumer1Updates(3) ++ consumer2Updates(1)) == consumer1Updates(1)
          }))
        } yield ()).compile.drain.unsafeRunSync()
      }
    }

    it("begin from the current assignments") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)

        (for {
          consumer <- consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.subscribeTo(topic))
          _ <- Stream.eval(IO.sleep(5.seconds)).concurrently(consumer.stream)
          queue <- Stream.eval(Queue.noneTerminated[IO, SortedSet[TopicPartition]])
          _ <- Stream.eval(
            consumer.assignmentStream.evalTap(as => queue.enqueue1(Some(as))).compile.drain.start
          )
          _ <- Stream.eval(IO.sleep(5.seconds))
          _ <- Stream.eval(queue.enqueue1(None))
          updates <- Stream.eval(queue.dequeue.compile.toList)
          _ <- Stream.eval(IO(assert {
            updates.length == 1 && updates.head.size == 3
          }))
        } yield ()).compile.drain.unsafeRunSync()
      }
    }
  }
}
