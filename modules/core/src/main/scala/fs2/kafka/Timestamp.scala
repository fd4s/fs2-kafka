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

import cats.Show

/**
  * [[Timestamp]] is an optional timestamp value representing
  * a [[createTime]], [[logAppendTime]], [[unknownTime]], or
  * no timestamp at all.
  */
sealed abstract class Timestamp {

  /**
    * Returns the timestamp value, if the timestamp is
    * representing the time when a record was created.
    */
  def createTime: Option[Long]

  /**
    * Returns the timestamp value, if the timestamp is
    * representing the time when a record was appended
    * to the log.
    */
  def logAppendTime: Option[Long]

  /**
    * Returns the timestamp value, if there is a
    * timestamp, but the type is unknown.
    */
  def unknownTime: Option[Long]

  /**
    * Returns `true` if there is no timestamp value; otherwise `false`.
    */
  def isEmpty: Boolean

  /**
    * Returns `true` if there is a timestamp value; otherwise `false`.
    */
  final def nonEmpty: Boolean =
    !isEmpty
}

object Timestamp {

  /**
    * Creates a new [[Timestamp]] instance from the specified
    * timestamp value representing the time when the record
    * was created.
    */
  def createTime(value: Long): Timestamp =
    new Timestamp {
      override val createTime: Option[Long] = Some(value)
      override val logAppendTime: Option[Long] = None
      override val unknownTime: Option[Long] = None
      override val isEmpty: Boolean = false
      override def toString: String = s"Timestamp(createTime = $value)"
    }

  /**
    * Creates a new [[Timestamp]] instance from the specified
    * timestamp value representing the time when the record
    * was appended to the log.
    */
  def logAppendTime(value: Long): Timestamp =
    new Timestamp {
      override val createTime: Option[Long] = None
      override val logAppendTime: Option[Long] = Some(value)
      override val unknownTime: Option[Long] = None
      override val isEmpty: Boolean = false
      override def toString: String = s"Timestamp(logAppendTime = $value)"
    }

  /**
    * Creates a new [[Timestamp]] instance from the specified
    * timestamp value, when the timestamp type is unknown.
    */
  def unknownTime(value: Long): Timestamp =
    new Timestamp {
      override val createTime: Option[Long] = None
      override val logAppendTime: Option[Long] = None
      override val unknownTime: Option[Long] = Some(value)
      override val isEmpty: Boolean = false
      override def toString: String = s"Timestamp(unknownTime = $value)"
    }

  /**
    * The [[Timestamp]] instance without any timestamp values.
    */
  val none: Timestamp =
    new Timestamp {
      override val createTime: Option[Long] = None
      override val logAppendTime: Option[Long] = None
      override val unknownTime: Option[Long] = None
      override val isEmpty: Boolean = true
      override def toString: String = "Timestamp()"
    }

  implicit val timestampShow: Show[Timestamp] =
    Show.fromToString
}
