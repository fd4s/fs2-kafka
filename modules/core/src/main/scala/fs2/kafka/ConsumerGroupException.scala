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

import org.apache.kafka.common.KafkaException

/**
  * [[ConsumerGroupException]] indicates that one of the two following
  * conditions occurred before records were produced transactionally
  * with the [[TransactionalKafkaProducer]].<br>
  * <br>
  * - There were [[CommittableOffset]]s without a consumer group ID.<br>
  * - There were [[CommittableOffset]]s for multiple consumer group IDs.
  */
sealed abstract class ConsumerGroupException(groupIds: Set[String])
    extends KafkaException({
      val groupIdsString = groupIds.toList.sorted.mkString(", ")
      s"multiple or missing consumer group ids in transaction [$groupIdsString]"
    })

private[kafka] object ConsumerGroupException {
  def apply(groupIds: Set[String]): ConsumerGroupException =
    new ConsumerGroupException(groupIds) {
      override def toString: String =
        s"fs2.kafka.ConsumerGroupException: $getMessage"
    }
}
