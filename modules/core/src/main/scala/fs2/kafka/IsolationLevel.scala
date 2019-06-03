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
  * The available options for [[ConsumerSettings#withIsolationLevel]].<br>
  * <br>
  * Available options include:<br>
  * - [[IsolationLevel#ReadCommitted]] to only read committed records,<br>
  * - [[IsolationLevel#ReadUncommitted]] to also read uncommitted records.
  */
sealed abstract class IsolationLevel

object IsolationLevel {
  private[kafka] case object ReadCommittedIsolationLevel extends IsolationLevel {
    override def toString: String = "ReadCommitted"
  }

  private[kafka] case object ReadUncommittedIsolationLevel extends IsolationLevel {
    override def toString: String = "ReadUncommitted"
  }

  /**
    * Option to only read committed records.
    */
  val ReadCommitted: IsolationLevel = ReadCommittedIsolationLevel

  /**
    * Option to read both committed and uncommitted records.
    */
  val ReadUncommitted: IsolationLevel = ReadUncommittedIsolationLevel
}
