/*
 * Copyright 2018-2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.kafka

import cats.effect._
import cats.Foldable
import fs2.kafka.internal.converters.collection._
import fs2.kafka.internal.syntax._
import fs2.kafka.internal.WithAdminClient
import fs2.kafka.KafkaAdminClient._
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.{Node, TopicPartition}

/**
  * [[KafkaAdminClient]] represents an admin client for Kafka, which is able to
  * describe queries about topics, consumer groups, offsets, and other entities
  * related to Kafka.<br>
  * <br>
  * Use [[adminClientResource]] or [[adminClientStream]] to create an instance.
  */
sealed abstract class KafkaAdminClient[F[_]] {

  /**
    * Creates the specified topic.
    */
  def createTopic(topic: NewTopic): F[Unit]

  /**
    * Creates the specified topics.
    */
  def createTopics[G[_]](topics: G[NewTopic])(
    implicit G: Foldable[G]
  ): F[Unit]

  /**
    * Describes the cluster. Returns nodes using:
    *
    * {{{
    * describeCluster.nodes
    * }}}
    *
    * or the controller node using:
    *
    * {{{
    * describeCluster.controller
    * }}}
    *
    * or the cluster ID using the following.
    *
    * {{{
    * describeCluster.clusterId
    * }}}
    */
  def describeCluster: DescribeCluster[F]

  /**
    * Describes the consumer groups with the specified group ids, returning a
    * `Map` with group ids as keys, and `ConsumerGroupDescription`s as values.
    */
  def describeConsumerGroups[G[_]](groupIds: G[String])(
    implicit G: Foldable[G]
  ): F[Map[String, ConsumerGroupDescription]]

  /**
    * Describes the topics with the specified topic names, returning a
    * `Map` with topic names as keys, and `TopicDescription`s as values.
    */
  def describeTopics[G[_]](topics: G[String])(
    implicit G: Foldable[G]
  ): F[Map[String, TopicDescription]]

  /**
    * Updates the configuration for the specified resources.
    */
  def alterConfigs[G[_]](configs: Map[ConfigResource, G[AlterConfigOp]])(
    implicit G: Foldable[G]
  ): F[Unit]

  /**
    * Lists consumer groups. Returns group ids using:
    *
    * {{{
    * listConsumerGroups.groupIds
    * }}}
    *
    * or `ConsumerGroupListing`s using the following.
    *
    * {{{
    * listConsumerGroups.listings
    * }}}
    */
  def listConsumerGroups: ListConsumerGroups[F]

  /**
    * Lists consumer group offsets. Returns offsets per topic-partition using:
    *
    * {{{
    * listConsumerGroupOffsets(groupId)
    *   .partitionsToOffsetAndMetadata
    * }}}
    *
    * or only offsets for specified topic-partitions using the following.
    *
    * {{{
    * listConsumerGroupOffsets(groupId)
    *   .forPartitions(topicPartitions)
    *   .partitionsToOffsetAndMetadata
    * }}}
    */
  def listConsumerGroupOffsets(groupId: String): ListConsumerGroupOffsets[F]

  /**
    * Lists topics. Returns topic names using:
    *
    * {{{
    * listTopics.names
    * }}}
    *
    * or `TopicListing`s using:
    *
    * {{{
    * listTopics.listings
    * }}}
    *
    * or a `Map` of topic names to `TopicListing`s using the following.
    *
    * {{{
    * listTopics.namesToListings
    * }}}
    *
    * If you want to include internal topics, first use `includeInternal`.
    *
    * {{{
    * listTopics.includeInternal.listings
    * }}}
    */
  def listTopics: ListTopics[F]

  /**
    * Increase the number of partitions for different topics
    */
  def createPartitions(newPartitions: Map[String, NewPartitions]): F[Unit]

  /**
    * Deletes the specified topic.
    */
  def deleteTopic(topic: String): F[Unit]

  /**
    * Deletes the specified topics.
    */
  def deleteTopics[G[_]](topics: G[String])(
    implicit G: Foldable[G]
  ): F[Unit]
}

object KafkaAdminClient {
  private[this] def createTopicWith[F[_]](
    withAdminClient: WithAdminClient[F],
    topic: NewTopic
  ): F[Unit] =
    withAdminClient(_.createTopics(java.util.Collections.singleton(topic)).all.void)

  private[this] def createTopicsWith[F[_], G[_]](
    withAdminClient: WithAdminClient[F],
    topics: G[NewTopic]
  )(implicit G: Foldable[G]): F[Unit] =
    withAdminClient(_.createTopics(topics.asJava).all.void)

  private[this] def describeConsumerGroupsWith[F[_], G[_]](
    withAdminClient: WithAdminClient[F],
    groupIds: G[String]
  )(implicit G: Foldable[G]): F[Map[String, ConsumerGroupDescription]] =
    withAdminClient(_.describeConsumerGroups(groupIds.asJava).all.map(_.toMap))

  private[this] def describeTopicsWith[F[_], G[_]](
    withAdminClient: WithAdminClient[F],
    topics: G[String]
  )(implicit G: Foldable[G]): F[Map[String, TopicDescription]] =
    withAdminClient(_.describeTopics(topics.asJava).all.map(_.toMap))

  private[this] def alterConfigsWith[F[_], G[_]](
    withAdminClient: WithAdminClient[F],
    configs: Map[ConfigResource, G[AlterConfigOp]]
  )(implicit G: Foldable[G]): F[Unit] =
    withAdminClient(_.incrementalAlterConfigs(configs.asJavaMap).all.void)

  sealed abstract class DescribeCluster[F[_]] {

    /** Lists available nodes in the cluster. */
    def nodes: F[Set[Node]]

    /** The node in the cluster acting as the current controller. */
    def controller: F[Node]

    /** Current cluster ID. */
    def clusterId: F[String]
  }

  private[this] def describeClusterWith[F[_]](
    withAdminClient: WithAdminClient[F]
  ): DescribeCluster[F] =
    new DescribeCluster[F] {
      override def nodes: F[Set[Node]] =
        withAdminClient(_.describeCluster.nodes.map(_.toSet))

      override def controller: F[Node] =
        withAdminClient(_.describeCluster.controller)

      override def clusterId: F[String] =
        withAdminClient(_.describeCluster.clusterId)

      override def toString: String =
        "DescribeCluster$" + System.identityHashCode(this)
    }

  sealed abstract class ListTopics[F[_]] {

    /** Lists topic names. */
    def names: F[Set[String]]

    /** Lists topics as `TopicListing`s. */
    def listings: F[List[TopicListing]]

    /** Lists topics as a `Map` from topic names to `TopicListing`s. */
    def namesToListings: F[Map[String, TopicListing]]

    /** Include internal topics in the listing. */
    def includeInternal: ListTopicsIncludeInternal[F]
  }

  sealed abstract class ListTopicsIncludeInternal[F[_]] {

    /** Lists topic names. Includes internal topics. */
    def names: F[Set[String]]

    /** Lists topics as `TopicListing`s. Includes internal topics. */
    def listings: F[List[TopicListing]]

    /** Lists topics as a `Map` from topic names to `TopicListing`s. Includes internal topics. */
    def namesToListings: F[Map[String, TopicListing]]
  }

  private[this] def listTopicsWith[F[_]](
    withAdminClient: WithAdminClient[F]
  ): ListTopics[F] =
    new ListTopics[F] {
      override def names: F[Set[String]] =
        withAdminClient(_.listTopics.names.map(_.toSet))

      override def listings: F[List[TopicListing]] =
        withAdminClient(_.listTopics.listings.map(_.toList))

      override def namesToListings: F[Map[String, TopicListing]] =
        withAdminClient(_.listTopics.namesToListings.map(_.toMap))

      override def includeInternal: ListTopicsIncludeInternal[F] =
        listTopicsIncludeInternalWith(withAdminClient)

      override def toString: String =
        "ListTopics$" + System.identityHashCode(this)
    }

  private[this] def listTopicsIncludeInternalWith[F[_]](
    withAdminClient: WithAdminClient[F]
  ): ListTopicsIncludeInternal[F] =
    new ListTopicsIncludeInternal[F] {
      private[this] def options: ListTopicsOptions =
        new ListTopicsOptions().listInternal(true)

      override def names: F[Set[String]] =
        withAdminClient(_.listTopics(options).names.map(_.toSet))

      override def listings: F[List[TopicListing]] =
        withAdminClient(_.listTopics(options).listings.map(_.toList))

      override def namesToListings: F[Map[String, TopicListing]] =
        withAdminClient(_.listTopics(options).namesToListings.map(_.toMap))

      override def toString: String =
        "ListTopicsIncludeInternal$" + System.identityHashCode(this)
    }

  sealed abstract class ListConsumerGroups[F[_]] {

    /** Lists the available consumer group ids. */
    def groupIds: F[List[String]]

    /** List the available consumer groups as `ConsumerGroupListing`s. */
    def listings: F[List[ConsumerGroupListing]]
  }

  private[this] def listConsumerGroupsWith[F[_]](
    withAdminClient: WithAdminClient[F]
  ): ListConsumerGroups[F] =
    new ListConsumerGroups[F] {
      override def groupIds: F[List[String]] =
        withAdminClient(_.listConsumerGroups.all.map(_.mapToList(_.groupId)))

      override def listings: F[List[ConsumerGroupListing]] =
        withAdminClient(_.listConsumerGroups.all.map(_.toList))

      override def toString: String =
        "ListConsumerGroups$" + System.identityHashCode(this)
    }

  sealed abstract class ListConsumerGroupOffsets[F[_]] {

    /** Lists consumer group offsets for the consumer group. */
    def partitionsToOffsetAndMetadata: F[Map[TopicPartition, OffsetAndMetadata]]

    /** Only includes consumer group offsets for specified topic-partitions. */
    def forPartitions[G[_]](
      partitions: G[TopicPartition]
    )(implicit G: Foldable[G]): ListConsumerGroupOffsetsForPartitions[F]
  }

  sealed abstract class ListConsumerGroupOffsetsForPartitions[F[_]] {

    /** Lists consumer group offsets on specified partitions for the consumer group. */
    def partitionsToOffsetAndMetadata: F[Map[TopicPartition, OffsetAndMetadata]]
  }

  private[this] def listConsumerGroupOffsetsWith[F[_]](
    withAdminClient: WithAdminClient[F],
    groupId: String
  ): ListConsumerGroupOffsets[F] =
    new ListConsumerGroupOffsets[F] {
      override def partitionsToOffsetAndMetadata: F[Map[TopicPartition, OffsetAndMetadata]] =
        withAdminClient { adminClient =>
          adminClient
            .listConsumerGroupOffsets(groupId)
            .partitionsToOffsetAndMetadata
            .map(_.toMap)
        }

      override def forPartitions[G[_]](
        partitions: G[TopicPartition]
      )(implicit G: Foldable[G]): ListConsumerGroupOffsetsForPartitions[F] =
        listConsumerGroupOffsetsForPartitionsWith(withAdminClient, groupId, partitions)

      override def toString: String =
        s"ListConsumerGroupOffsets(groupId = $groupId)"
    }

  private[this] def listConsumerGroupOffsetsForPartitionsWith[F[_], G[_]](
    withAdminClient: WithAdminClient[F],
    groupId: String,
    partitions: G[TopicPartition]
  )(implicit G: Foldable[G]): ListConsumerGroupOffsetsForPartitions[F] =
    new ListConsumerGroupOffsetsForPartitions[F] {
      private[this] def options: ListConsumerGroupOffsetsOptions =
        new ListConsumerGroupOffsetsOptions().topicPartitions(partitions.asJava)

      override def partitionsToOffsetAndMetadata: F[Map[TopicPartition, OffsetAndMetadata]] =
        withAdminClient { adminClient =>
          adminClient
            .listConsumerGroupOffsets(groupId, options)
            .partitionsToOffsetAndMetadata
            .map(_.toMap)
        }

      override def toString: String =
        s"ListConsumerGroupOffsetsForPartitions(groupId = $groupId, partitions = $partitions)"
    }

  private[this] def createPartitionsWith[F[_]](
    withAdminClient: WithAdminClient[F],
    newPartitions: Map[String, NewPartitions]
  ): F[Unit] =
    withAdminClient(_.createPartitions(newPartitions.asJava).all.void)

  private[this] def deleteTopicWith[F[_]](
    withAdminClient: WithAdminClient[F],
    topic: String
  ): F[Unit] =
    withAdminClient(_.deleteTopics(java.util.Collections.singleton(topic)).all.void)

  private[this] def deleteTopicsWith[F[_], G[_]](
    withAdminClient: WithAdminClient[F],
    topics: G[String]
  )(implicit G: Foldable[G]): F[Unit] =
    withAdminClient(_.deleteTopics(topics.asJava).all.void)

  private[kafka] def resource[F[_]](
    settings: AdminClientSettings[F]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, KafkaAdminClient[F]] =
    WithAdminClient(settings).map { client =>
      new KafkaAdminClient[F] {
        override def createTopic(topic: NewTopic): F[Unit] =
          createTopicWith(client, topic)

        override def createTopics[G[_]](topics: G[NewTopic])(
          implicit G: Foldable[G]
        ): F[Unit] =
          createTopicsWith(client, topics)

        override def describeCluster: DescribeCluster[F] =
          describeClusterWith(client)

        override def describeConsumerGroups[G[_]](groupIds: G[String])(
          implicit G: Foldable[G]
        ): F[Map[String, ConsumerGroupDescription]] =
          describeConsumerGroupsWith(client, groupIds)

        override def describeTopics[G[_]](topics: G[String])(
          implicit G: Foldable[G]
        ): F[Map[String, TopicDescription]] =
          describeTopicsWith(client, topics)

        override def alterConfigs[G[_]](configs: Map[ConfigResource, G[AlterConfigOp]])(
          implicit G: Foldable[G]
        ): F[Unit] =
          alterConfigsWith(client, configs)

        override def listConsumerGroups: ListConsumerGroups[F] =
          listConsumerGroupsWith(client)

        override def listConsumerGroupOffsets(groupId: String): ListConsumerGroupOffsets[F] =
          listConsumerGroupOffsetsWith(client, groupId)

        override def listTopics: ListTopics[F] =
          listTopicsWith(client)

        override def createPartitions(newPartitions: Map[String, NewPartitions]): F[Unit] =
          createPartitionsWith(client, newPartitions)

        override def deleteTopic(topic: String): F[Unit] =
          deleteTopicWith(client, topic)

        override def deleteTopics[G[_]](topics: G[String])(implicit G: Foldable[G]): F[Unit] =
          deleteTopicsWith(client, topics)

        override def toString: String =
          "KafkaAdminClient$" + System.identityHashCode(this)
      }
    }
}
