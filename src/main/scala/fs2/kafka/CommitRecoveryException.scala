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

import cats.instances.list._
import cats.syntax.show._
import fs2.kafka.internal.instances._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{KafkaException, TopicPartition}

/**
  * [[CommitRecoveryException]] indicates that offset commit recovery was
  * attempted `attempts` times for `offsets`, but that it wasn't able to
  * complete successfully. The last encountered exception is provided as
  * `lastException`.<br>
  * <br>
  * Use [[CommitRecoveryException#apply]] to create a new instance.
  */
sealed abstract class CommitRecoveryException(
  attempts: Int,
  lastException: Throwable,
  offsets: Map[TopicPartition, OffsetAndMetadata]
) extends KafkaException({
      offsets.toList.sorted.mkStringAppend {
        case (append, (tp, oam)) =>
          append(tp.show)
          append(" -> ")
          append(oam.show)
      }(
        start =
          s"offset commit is still failing after $attempts attempts${if (offsets.nonEmpty) " for offsets: "
          else ""}",
        sep = ", ",
        end = s"; last exception was: $lastException"
      )
    })

object CommitRecoveryException {

  /**
    * Creates a new [[CommitRecoveryException]] indicating that offset
    * commit recovery was attempted `attempts` times for `offsets` but
    * that it wasn't able to complete successfully. The last exception
    * encountered was `lastException`.
    */
  def apply(
    attempts: Int,
    lastException: Throwable,
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): CommitRecoveryException =
    new CommitRecoveryException(attempts, lastException, offsets) {
      override def toString: String =
        s"fs2.kafka.CommitRecoveryException: $getMessage"
    }
}
