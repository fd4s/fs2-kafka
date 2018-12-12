package fs2.kafka

import cats.effect.IO
import cats.implicits._
import fs2.Stream
import org.apache.kafka.common.TopicPartition

final class KafkaAdminClientSpec extends BaseKafkaSpec {
  describe("KafkaAdminClient") {
    it("should support all defined functionality") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        (for {
          consumerSettings <- consumerSettings(config)
          consumer <- consumerStream[IO].using(consumerSettings)
          _ <- Stream.eval(consumer.subscribe(topic.r))
          _ <- consumer.stream
            .take(produced.size.toLong)
            .map(_.committableOffset)
            .to(commitBatch)
        } yield ()).compile.lastOrError.unsafeRunSync

        adminClientResource[IO](adminClientSettings(config)).use { adminClient =>
          for {
            consumerGroupIds <- adminClient.listConsumerGroups.groupIds
            _ <- IO(assert(consumerGroupIds.size == 1))
            consumerGroupListings <- adminClient.listConsumerGroups.listings
            _ <- IO(assert(consumerGroupListings.size == 1))
            describedConsumerGroups <- adminClient.describeConsumerGroups(consumerGroupIds)
            _ <- IO(assert(describedConsumerGroups.size == 1))
            consumerGroupOffsets <- consumerGroupIds.parTraverse { groupId =>
              adminClient
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata
                .map((groupId, _))
            }
            _ <- IO(assert(consumerGroupOffsets.size == 1))
            consumerGroupOffsetsPartitions <- consumerGroupIds.parTraverse { groupId =>
              adminClient
                .listConsumerGroupOffsets(groupId)
                .forPartitions(List.empty[TopicPartition])
                .partitionsToOffsetAndMetadata
                .map((groupId, _))
            }
            _ <- IO(assert(consumerGroupOffsetsPartitions.size == 1))
            topicNames <- adminClient.listTopics.names
            _ <- IO(assert(topicNames.size == 1))
            topicListings <- adminClient.listTopics.listings
            _ <- IO(assert(topicListings.size == 1))
            topicNamesToListings <- adminClient.listTopics.namesToListings
            _ <- IO(assert(topicNamesToListings.size == 1))
            topicNamesInternal <- adminClient.listTopics.includeInternal.names
            _ <- IO(assert(topicNamesInternal.size == 2))
            topicListingsInternal <- adminClient.listTopics.includeInternal.listings
            _ <- IO(assert(topicListingsInternal.size == 2))
            topicNamesToListingsInternal <- adminClient.listTopics.includeInternal.namesToListings
            _ <- IO(assert(topicNamesToListingsInternal.size == 2))
            describedTopics <- adminClient.describeTopics(topicNames.toList)
            _ <- IO(assert(describedTopics.size == 1))
          } yield ()
        }.unsafeRunSync
      }
    }
  }
}
