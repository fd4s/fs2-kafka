/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.data.{Chain, NonEmptyList, NonEmptySet, NonEmptyVector}
import cats.effect.Deferred
import cats.implicits._
import fs2.Chunk
import fs2.kafka.CommittableConsumerRecord
import fs2.kafka.internal.instances._
import fs2.kafka.internal.KafkaConsumerActor._
import fs2.kafka.internal.LogLevel._
import fs2.kafka.internal.syntax._
import java.util.regex.Pattern
import org.apache.kafka.common.TopicPartition
import scala.collection.immutable.SortedSet

private[kafka] sealed abstract class LogEntry {
  def level: LogLevel

  def message: String
}

private[kafka] object LogEntry {
  final case class SubscribedTopics[F[_], K, V](
    topics: NonEmptyList[String],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Consumer subscribed to topics [${topics.toList.mkString(", ")}]. Current state [$state]."
  }

  final case class ManuallyAssignedPartitions[F[_], K, V](
    partitions: NonEmptySet[TopicPartition],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Consumer manually assigned partitions [${partitions.toList.mkString(", ")}]. Current state [$state]."
  }

  final case class SubscribedPattern[F[_], K, V](
    pattern: Pattern,
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Consumer subscribed to pattern [$pattern]. Current state [$state]."
  }

  final case class Unsubscribed[F[_], K, V](
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Consumer unsubscribed from all partitions. Current state [$state]."
  }

  final case class StoredFetch[F[_], K, V, A](
    partition: TopicPartition,
    callback: ((Chunk[CommittableConsumerRecord[F, K, V]], FetchCompletedReason)) => F[Unit],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Stored fetch [$callback] for partition [$partition]. Current state [$state]."
  }

  final case class StoredOnRebalance[F[_], K, V](
    onRebalance: OnRebalance[F, K, V],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Stored OnRebalance [$onRebalance]. Current state [$state]."
  }

  final case class AssignedPartitions[F[_], K, V](
    partitions: SortedSet[TopicPartition],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Assigned partitions [${partitions.mkString(", ")}]. Current state [$state]."
  }

  final case class RevokedPartitions[F[_], K, V](
    partitions: SortedSet[TopicPartition],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Revoked partitions [${partitions.mkString(", ")}]. Current state [$state]."
  }

  final case class CompletedFetchesWithRecords[F[_], K, V](
    records: Records[F, K, V],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Completed fetches with records for partitions [${recordsString(records)}]. Current state [$state]."
  }

  final case class RevokedFetchesWithRecords[F[_], K, V](
    records: Records[F, K, V],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Revoked fetches with records for partitions [${recordsString(records)}]. Current state [$state]."
  }

  final case class RevokedFetchesWithoutRecords[F[_], K, V](
    partitions: Set[TopicPartition],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Revoked fetches without records for partitions [${partitions.mkString(", ")}]. Current state [$state]."
  }

  final case class RemovedRevokedRecords[F[_], K, V](
    records: Records[F, K, V],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Removed revoked records for partitions [${recordsString(records)}]. Current state [$state]."
  }

  final case class StoredRecords[F[_], K, V](
    records: Records[F, K, V],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Stored records for partitions [${recordsString(records)}]. Current state [$state]."
  }

  final case class RevokedPreviousFetch(
    partition: TopicPartition,
    streamId: StreamId
  ) extends LogEntry {
    override def level: LogLevel = Warn
    override def message: String =
      s"Revoked previous fetch for partition [$partition] in stream with id [$streamId]."
  }

  final case class StoredPendingCommit[F[_], K, V](
    commit: Request.Commit[F, K, V],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Stored pending commit [$commit] as rebalance is in-progress. Current state [$state]."
  }

  final case class CommittedPendingCommits[F[_], K, V](
    pendingCommits: Chain[Request.Commit[F, K, V]],
    state: State[F, K, V]
  ) extends LogEntry {
    override def level: LogLevel = Debug
    override def message: String =
      s"Committed pending commits [$pendingCommits]. Current state [$state]."
  }

  def recordsString[F[_], K, V](
    records: Records[F, K, V]
  ): String =
    records.toList
      .sortBy { case (tp, _) => tp }
      .mkStringAppend {
        case (append, (tp, ms)) =>
          append(tp.show)
          append(" -> { first: ")
          append(ms.head.offset.offsetAndMetadata.show)
          append(", last: ")
          append(ms.last.offset.offsetAndMetadata.show)
          append(" }")
      }("", ", ", "")

  private[this] type Records[F[_], K, V] =
    Map[TopicPartition, NonEmptyVector[CommittableConsumerRecord[F, K, V]]]
}
