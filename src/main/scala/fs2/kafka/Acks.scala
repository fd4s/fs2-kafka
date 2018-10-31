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

sealed abstract class Acks

object Acks {
  private[kafka] case object ZeroAcks extends Acks

  private[kafka] case object OneAcks extends Acks

  private[kafka] case object AllAcks extends Acks

  val Zero: Acks = ZeroAcks

  val One: Acks = OneAcks

  val All: Acks = AllAcks
}
