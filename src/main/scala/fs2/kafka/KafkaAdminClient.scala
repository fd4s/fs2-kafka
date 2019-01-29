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

import cats.Foldable
import cats.effect.syntax.concurrent._
import cats.effect.{Concurrent, Resource}
import cats.syntax.flatMap._
import fs2.kafka.KafkaAdminClient._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{KafkaFuture, TopicPartition}

/**
  * [[KafkaAdminClient]] represents an admin client for Kafka, which is able to
  * describe queries about topics, consumer groups, offsets, and other entities
  * related to Kafka.<br>
  * <br>
  * Use [[adminClientResource]] or [[adminClientStream]] to create an instance.
  */
sealed abstract class KafkaAdminClient[F[_]] {

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
}

object KafkaAdminClient {
  private[this] def describeConsumerGroupsWith[F[_], G[_]](
    client: Client[F],
    groupIds: G[String]
  )(implicit G: Foldable[G]): F[Map[String, ConsumerGroupDescription]] =
    client(_.describeConsumerGroups(groupIds.asJava).all.map(_.toMap))

  private[this] def describeTopicsWith[F[_], G[_]](
    client: Client[F],
    topics: G[String]
  )(implicit G: Foldable[G]): F[Map[String, TopicDescription]] =
    client(_.describeTopics(topics.asJava).all.map(_.toMap))

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
    client: Client[F]
  ): ListTopics[F] =
    new ListTopics[F] {
      override def names: F[Set[String]] =
        client(_.listTopics.names.map(_.toSet))

      override def listings: F[List[TopicListing]] =
        client(_.listTopics.listings.map(_.toList))

      override def namesToListings: F[Map[String, TopicListing]] =
        client(_.listTopics.namesToListings.map(_.toMap))

      override def includeInternal: ListTopicsIncludeInternal[F] =
        listTopicsIncludeInternalWith(client)

      override def toString: String =
        "ListTopics$" + System.identityHashCode(this)
    }

  private[this] def listTopicsIncludeInternalWith[F[_]](
    client: Client[F]
  ): ListTopicsIncludeInternal[F] =
    new ListTopicsIncludeInternal[F] {
      private[this] def options: ListTopicsOptions =
        new ListTopicsOptions().listInternal(true)

      override def names: F[Set[String]] =
        client(_.listTopics(options).names.map(_.toSet))

      override def listings: F[List[TopicListing]] =
        client(_.listTopics(options).listings.map(_.toList))

      override def namesToListings: F[Map[String, TopicListing]] =
        client(_.listTopics(options).namesToListings.map(_.toMap))

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
    client: Client[F]
  ): ListConsumerGroups[F] =
    new ListConsumerGroups[F] {
      override def groupIds: F[List[String]] =
        client(_.listConsumerGroups.all.map(_.mapToList(_.groupId)))

      override def listings: F[List[ConsumerGroupListing]] =
        client(_.listConsumerGroups.all.map(_.toList))

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
    client: Client[F],
    groupId: String
  ): ListConsumerGroupOffsets[F] =
    new ListConsumerGroupOffsets[F] {
      override def partitionsToOffsetAndMetadata: F[Map[TopicPartition, OffsetAndMetadata]] =
        client { adminClient =>
          adminClient
            .listConsumerGroupOffsets(groupId)
            .partitionsToOffsetAndMetadata
            .map(_.toMap)
        }

      override def forPartitions[G[_]](
        partitions: G[TopicPartition]
      )(implicit G: Foldable[G]): ListConsumerGroupOffsetsForPartitions[F] =
        listConsumerGroupOffsetsForPartitionsWith(client, groupId, partitions)

      override def toString: String =
        s"ListConsumerGroupOffsets(groupId = $groupId)"
    }

  private[this] def listConsumerGroupOffsetsForPartitionsWith[F[_], G[_]](
    client: Client[F],
    groupId: String,
    partitions: G[TopicPartition]
  )(implicit G: Foldable[G]): ListConsumerGroupOffsetsForPartitions[F] =
    new ListConsumerGroupOffsetsForPartitions[F] {
      private[this] def options: ListConsumerGroupOffsetsOptions =
        new ListConsumerGroupOffsetsOptions().topicPartitions(partitions.asJava)

      override def partitionsToOffsetAndMetadata: F[Map[TopicPartition, OffsetAndMetadata]] =
        client { adminClient =>
          adminClient
            .listConsumerGroupOffsets(groupId, options)
            .partitionsToOffsetAndMetadata
            .map(_.toMap)
        }

      override def toString: String =
        s"ListConsumerGroupOffsetsForPartitions(groupId = $groupId, partitions = $partitions)"
    }

  private[this] def createAdminClient[F[_]](
    settings: AdminClientSettings
  )(implicit F: Concurrent[F]): Resource[F, Client[F]] =
    Resource
      .make[F, AdminClient] {
        settings.adminClientFactory
          .create(settings)
      } { adminClient =>
        F.delay {
            adminClient.close(
              settings.closeTimeout.length,
              settings.closeTimeout.unit
            )
          }
          .start
          .flatMap(_.join)
      }
      .map(new Client(_))

  private[this] final class Client[F[_]](
    adminClient: AdminClient
  )(implicit F: Concurrent[F]) {
    def apply[A](f: AdminClient => KafkaFuture[A]): F[A] =
      F.suspend(f(adminClient).cancelable)
  }

  private[kafka] def adminClientResource[F[_]](
    settings: AdminClientSettings
  )(implicit F: Concurrent[F]): Resource[F, KafkaAdminClient[F]] =
    createAdminClient(settings).map { client =>
      new KafkaAdminClient[F] {
        def describeConsumerGroups[G[_]](groupIds: G[String])(
          implicit G: Foldable[G]
        ): F[Map[String, ConsumerGroupDescription]] =
          describeConsumerGroupsWith(client, groupIds)

        def describeTopics[G[_]](topics: G[String])(
          implicit G: Foldable[G]
        ): F[Map[String, TopicDescription]] =
          describeTopicsWith(client, topics)

        override def listConsumerGroups: ListConsumerGroups[F] =
          listConsumerGroupsWith(client)

        override def listConsumerGroupOffsets(groupId: String): ListConsumerGroupOffsets[F] =
          listConsumerGroupOffsetsWith(client, groupId)

        override def listTopics: ListTopics[F] =
          listTopicsWith(client)

        override def toString: String =
          "KafkaAdminClient$" + System.identityHashCode(this)
      }
    }
}
