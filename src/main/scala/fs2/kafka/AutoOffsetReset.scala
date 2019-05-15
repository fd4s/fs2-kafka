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

/**
  * The available options for [[ConsumerSettings#withAutoOffsetReset]].<br>
  * <br>
  * Available options include:<br>
  * - [[AutoOffsetReset#Earliest]] to reset to the earliest offsets,<br>
  * - [[AutoOffsetReset#Latest]] to reset to the latest offsets,<br>
  * - [[AutoOffsetReset#None]] to fail if no offsets are available.
  */
sealed abstract class AutoOffsetReset

object AutoOffsetReset {
  private[kafka] case object EarliestOffsetReset extends AutoOffsetReset {
    override def toString: String = "Earliest"
  }

  private[kafka] case object LatestOffsetReset extends AutoOffsetReset {
    override def toString: String = "Latest"
  }

  private[kafka] case object NoneOffsetReset extends AutoOffsetReset {
    override def toString: String = "None"
  }

  /**
    * Option to reset to the earliest available offsets if no
    * initial or current offsets exist for the consumer group.
    */
  val Earliest: AutoOffsetReset = EarliestOffsetReset

  /**
    * Option to reset to the latest available offsets if no
    * initial or current offsets exist for the consumer group.
    */
  val Latest: AutoOffsetReset = LatestOffsetReset

  /**
    * Option to fail the consumer if there are no offsets
    * available for the consumer group.
    */
  val None: AutoOffsetReset = NoneOffsetReset
}
