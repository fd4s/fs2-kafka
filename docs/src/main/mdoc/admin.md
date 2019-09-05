---
id: admin
title: Admin
---

fs2-kafka also supports part of the Kafka admin API through
[the KafkaAdminClient][kafkaadminclient]. Under the hood, fs2-kafka makes use of Kafka's
[AdminClient][admin-client] and supports the same set of [settings](#settings).

The following imports are assumed throughout this page.

```scala mdoc:silent
import cats.effect._
import cats.implicits._
import fs2.Stream
import fs2.kafka._
```

## Settings

For the admin client settings, fs2-kafka provides a utility called
[AdminClientSettings][adminclientsettings] to avoid having to deal with string key value properties.

```scala mdoc:silent
def adminClientSettings[F[_]: Sync](bootstrapServers: String): AdminClientSettings[F] =
  AdminClientSettings[F].withBootstrapServers(bootstrapServers)
```

## Admin client creation

Once our settings are good to go, we can create our admin client, if inside a `Stream` context,
we can use [adminClientStream][adminclientstream]:

```scala mdoc:silent
def kafkaAdminClientStream[F[_]: Concurrent: ContextShift](
  bootstrapServers: String
): Stream[F, KafkaAdminClient[F]] =
  adminClientStream(adminClientSettings[F](bootstrapServers))
```

If outside of a `Stream` context, we can leverage [adminClientResource][adminclientresource]:

```scala mdoc:silent
def kafkaAdminClientRes[F[_]: Concurrent: ContextShift](
  bootstrapServers: String
): Resource[F, KafkaAdminClient[F]] =
  adminClientResource(adminClientSettings[F](bootstrapServers))
```

Now that we have our admin client, let's see what we can do with it.

## Topic-related functionalities

`KafkaAdminClient` supports a set of topic-related functionalities such as:

```scala mdoc:silent
import org.apache.kafka.clients.admin.{NewPartitions, NewTopic}

def topicOperations[F[_]: Concurrent: ContextShift]: F[Unit] =
  kafkaAdminClientRes[F]("localhost:9092").use { c =>
    for {
      topicNames <- c.listTopics.names
      _ <- c.describeTopics(topicNames.toList)
      _ <- c.createTopic(new NewTopic("new-topic", 1, 1)) // numPartitions and replicationFactor
      _ <- c.createTopics(new NewTopic("newer-topic", 1, 1) :: Nil)
      _ <- c.createPartitions(Map("new-topic" -> NewPartitions.increaseTo(4)))
      _ <- c.deleteTopic("new-topic")
      _ <- c.deleteTopics("newer-topic" :: Nil)
    } yield ()
  }
```

Note that the `listTopics` operation returns [ListTopics[F]][listtopics].

## Configuration-related features

Through `KafkaAdminClient` we can also edit the configurations of different resources like topics
or brokers:

```scala mdoc:silent
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}

def configOperations[F[_]: Concurrent: ContextShift]: F[Unit] =
  kafkaAdminClientRes[F]("localhost:9092").use { c =>
    for {
      cr <- Sync[F].pure(new ConfigResource(ConfigResource.Type.TOPIC, "topic"))
      _ <- c.describeConfigs(cr :: Nil)
      _ <- c.alterConfigs(Map(cr -> List(
        new AlterConfigOp(new ConfigEntry("cleanup.policy", "delete"), AlterConfigOp.OpType.SET))))
    } yield ()
  }
```

## Cluster information

We can also retrieve information from our cluster of brokers:

```scala mdoc:silent
import org.apache.kafka.common.Node

def clusterInfo[F[_]: Concurrent: ContextShift]: F[Set[Node]] =
  kafkaAdminClientRes[F]("localhost:9092").use(_.describeCluster.nodes)
```

`describeCluster` returns [DescribeCluster[F]][describecluster].

## Consumer group operations

There is also a set of operations we can perform relative to consumer groups:

```scala mdoc:silent
import cats.Parallel

def consumerGroupOperations[F[_]: Concurrent: ContextShift: Parallel]: F[Unit] =
  kafkaAdminClientRes[F]("localhost:9092").use { c =>
    for {
      consumerGroupIds <- c.listConsumerGroups.groupIds
      _ <- c.describeConsumerGroups(consumerGroupIds)
      _ <- consumerGroupIds.parTraverse { groupId =>
        c.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata
      }
    } yield ()
  }
```

`listConsumerGroups` returns [ListConsumerGroups[F]][listconsumergroups] while
`listConsumerGroupOffsets` returns [ListConsumerGroupOffsets[F]][listconsumergroupoffsets].

[kafkaadminclient]: @API_BASE_URL@/KafkaAdminClient.html
[adminclientsettings]: @API_BASE_URL@/AdminClientSettings.html
[adminclientstream]: @API_BASE_URL@/index.html#adminClientStream[F[_]](settings:fs2.kafka.AdminClientSettings[F])(implicitF:cats.effect.Concurrent[F],implicitcontext:cats.effect.ContextShift[F]):fs2.Stream[F,fs2.kafka.KafkaAdminClient[F]]
[adminclientsource]: @API_BASE_URL@/index.html#adminClientResource[F[_]](settings:fs2.kafka.AdminClientSettings[F])(implicitF:cats.effect.Concurrent[F],implicitcontext:cats.effect.ContextShift[F]):cats.effect.Resource[F,fs2.kafka.KafkaAdminClient[F]]
[listtopics]: @API_BASE_URL@/KafkaAdminClient$$ListTopics.html
[describecluster]: @API_BASE_URL@/KafkaAdminClient$$DescribeCluster.html
[listconsumergroups]: @API_BASE_URL@/KafkaAdminClient$$ListConsumerGroups.html
[listconsumergroupoffsets]: @API_BASE_URL@/KafkaAdminClient$$ListConsumerGroupOffsets.html
[admin-client]: @KAFKA_API_BASE_URL@/?org/apache/kafka/clients/admin/AdminClient.html
