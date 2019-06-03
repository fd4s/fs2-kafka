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
  * [[UnexpectedTopicException]] is raised when serialization or
  * deserialization occurred for an unexpected topic which isn't
  * supported by the [[Serializer]] or [[Deserializer]].
  */
sealed abstract class UnexpectedTopicException(topic: String)
    extends KafkaException(s"unexpected topic [$topic]")

private[kafka] object UnexpectedTopicException {
  def apply(topic: String): UnexpectedTopicException =
    new UnexpectedTopicException(topic) {
      override def toString: String =
        s"fs2.kafka.UnexpectedTopicException: $getMessage"
    }
}
