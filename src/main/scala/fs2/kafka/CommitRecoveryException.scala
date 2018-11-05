/*
 * Copyright 2018 OVO Energy Ltd
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

import cats.syntax.show._
import fs2.kafka.internal.instances._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{KafkaException, TopicPartition}

sealed abstract class CommitRecoveryException(
  attempts: Int,
  lastException: Throwable,
  offsets: Map[TopicPartition, OffsetAndMetadata]
) extends KafkaException({
      val builder =
        new StringBuilder(s"offset commit is still failing after $attempts attempts")

      if (offsets.nonEmpty) {
        builder.append(" for offsets: ")
        val sorted = offsets.toList.sorted
        var first = true

        sorted.foreach {
          case (tp, oam) =>
            if (first) first = false
            else builder.append(", ")

            builder.append(tp.show).append(" -> ").append(oam.show)
        }
      }

      builder.append(s"; last exception was: $lastException").toString
    }) {

  override def toString: String =
    s"fs2.kafka.CommitRecoveryException: $getMessage"
}

object CommitRecoveryException {
  def apply(
    attempts: Int,
    lastException: Throwable,
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): CommitRecoveryException =
    new CommitRecoveryException(attempts, lastException, offsets) {}
}
