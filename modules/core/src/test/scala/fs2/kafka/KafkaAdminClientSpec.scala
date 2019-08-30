package fs2.kafka

import cats.effect.IO
import cats.implicits._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry, NewTopic}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.clients.admin.NewPartitions

final class KafkaAdminClientSpec extends BaseKafkaSpec {
  describe("KafkaAdminClient") {
    it("should support consumer groups-related functionalities") {
      withKafka { (config, topic) =>
        commonSetup(topic, config)

        adminClientResource[IO](adminClientSettings(config)).use { adminClient =>
          for {
            consumerGroupIds <- adminClient.listConsumerGroups.groupIds
            _ <- IO(assert(consumerGroupIds.size == 1))
            consumerGroupListings <- adminClient.listConsumerGroups.listings
            _ <- IO(assert(consumerGroupListings.size == 1))
            describedConsumerGroups <- adminClient.describeConsumerGroups(consumerGroupIds)
            _ <- IO(assert(describedConsumerGroups.size == 1))
            _ <- IO {
              adminClient.listConsumerGroups.toString should
                startWith("ListConsumerGroups$")
            }
            consumerGroupOffsets <- consumerGroupIds.parTraverse { groupId =>
              adminClient
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata
                .map((groupId, _))
            }
            _ <- IO(assert(consumerGroupOffsets.size == 1))
            _ <- IO {
              adminClient
                .listConsumerGroupOffsets("group")
                .toString shouldBe "ListConsumerGroupOffsets(groupId = group)"
            }
            consumerGroupOffsetsPartitions <- consumerGroupIds.parTraverse { groupId =>
              adminClient
                .listConsumerGroupOffsets(groupId)
                .forPartitions(List.empty[TopicPartition])
                .partitionsToOffsetAndMetadata
                .map((groupId, _))
            }
            _ <- IO(assert(consumerGroupOffsetsPartitions.size == 1))
            _ <- IO {
              adminClient
                .listConsumerGroupOffsets("group")
                .forPartitions(List(new TopicPartition("topic", 0)))
                .toString shouldBe "ListConsumerGroupOffsetsForPartitions(groupId = group, partitions = List(topic-0))"
            }
          } yield ()
        }.unsafeRunSync()
      }
    }

    it("should support cluster-related functionalities") {
      withKafka { (config, topic) =>
        commonSetup(topic, config)

        adminClientResource[IO](adminClientSettings(config)).use { adminClient =>
          for {
            clusterNodes <- adminClient.describeCluster.nodes
            _ <- IO(assert(clusterNodes.size == 1))
            clusterController <- adminClient.describeCluster.controller
            _ <- IO(assert(!clusterController.isEmpty))
            clusterId <- adminClient.describeCluster.clusterId
            _ <- IO(assert(clusterId.nonEmpty))
            _ <- IO {
              adminClient.describeCluster.toString should startWith("DescribeCluster$")
            }
          } yield ()
        }.unsafeRunSync()
      }
    }

    it("should support config-related functionalities") {
      withKafka { (config, topic) =>
        commonSetup(topic, config)

        adminClientResource[IO](adminClientSettings(config)).use { adminClient =>
          for {
            cr <- IO.pure(new ConfigResource(ConfigResource.Type.TOPIC, topic))
            ce = new ConfigEntry("cleanup.policy", "delete")
            alteredConfigs <- adminClient.alterConfigs {
              Map(cr -> List(new AlterConfigOp(ce, OpType.SET)))
            }.attempt
            _ <- IO(assert(alteredConfigs.isRight))
            describedConfigs <- adminClient.describeConfigs(List(cr)).attempt
            _ <- IO(assert(describedConfigs.toOption.flatMap(_.get(cr)).map(_.contains(ce)).getOrElse(false)))
          } yield ()
        }.unsafeRunSync()
      }
    }

    it("should support topic-related functionalities") {
      withKafka { (config, topic) =>
        commonSetup(topic, config)

        adminClientResource[IO](adminClientSettings(config)).use { adminClient =>
          for {
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
            _ <- IO {
              adminClient.listTopics.toString should startWith("ListTopics$")
            }
            _ <- IO {
              adminClient.listTopics.includeInternal.toString should
                startWith("ListTopicsIncludeInternal$")
            }
            describedTopics <- adminClient.describeTopics(topicNames.toList)
            _ <- IO(assert(describedTopics.size == 1))
            newTopic = new NewTopic("new-test-topic", 1, 1)
            preCreateNames <- adminClient.listTopics.names
            _ <- IO(assert(!preCreateNames.contains(newTopic.name)))
            _ <- adminClient.createTopic(newTopic)
            postCreateNames <- adminClient.listTopics.names
            createAgain <- adminClient.createTopics(List(newTopic)).attempt
            _ <- IO(assert(createAgain.isLeft))
            _ <- IO(assert(postCreateNames.contains(newTopic.name)))
            createPartitions <- adminClient
              .createPartitions(Map(topic -> NewPartitions.increaseTo(4)))
              .attempt
            _ <- IO(assert(createPartitions.isRight))
            describedTopics <- adminClient.describeTopics(topic :: Nil)
            _ <- IO(assert(describedTopics.size == 1))
            _ <- IO(
              assert(describedTopics.headOption.map(_._2.partitions.size == 4).getOrElse(false))
            )
            deleteTopics <- adminClient
              .deleteTopics(List(topic))
              .attempt
            _ <- IO(assert(deleteTopics.isRight))
            describedTopics <- adminClient.describeTopics(topic :: Nil).attempt
            _ <- IO(
              assert(
                describedTopics.leftMap(_.getMessage()) == Left(
                  "This server does not host this topic-partition."
                )
              )
            )
            deleteTopic <- adminClient
              .deleteTopic(newTopic.name())
              .attempt
            _ <- IO(assert(deleteTopic.isRight))
            describedTopic <- adminClient.describeTopics(newTopic.name() :: Nil).attempt
            _ <- IO(
              assert(
                describedTopic.leftMap(_.getMessage()) == Left(
                  "This server does not host this topic-partition."
                )
              )
            )
          } yield ()
        }.unsafeRunSync()
      }
    }

    it("should support misc defined functionality") {
      withKafka { (config, topic) =>
        commonSetup(topic, config)

        adminClientResource[IO](adminClientSettings(config)).use { adminClient =>
          for {
            _ <- IO {
              adminClient.toString should startWith("KafkaAdminClient$")
            }
          } yield ()
        }.unsafeRunSync
      }
    }
  }

  def commonSetup(topic: String, config: EmbeddedKafkaConfig): Unit = {
    createCustomTopic(topic, partitions = 3)
    val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
    publishToKafka(topic, produced)

    consumerStream[IO]
      .using(consumerSettings(config))
      .evalTap(_.subscribe(topic.r))
      .flatMap(_.stream)
      .take(produced.size.toLong)
      .map(_.offset)
      .chunks
      .evalMap(CommittableOffsetBatch.fromFoldable(_).commit)
      .compile
      .lastOrError
      .unsafeRunSync
  }
}
