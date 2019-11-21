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
import org.apache.kafka.clients.producer.ProducerConfig
import scala.concurrent.duration.FiniteDuration

/**
  * [[TransactionalProducerSettings]] contain settings necessary to create a
  * [[TransactionalKafkaProducer]]. This includes a transactional ID and any
  * other [[ProducerSettings]].
  *
  * [[TransactionalProducerSettings]] instances are immutable and modification
  * functions return a new [[TransactionalProducerSettings]] instance.
  *
  * Use [[TransactionalProducerSettings.apply]] to create a new instance.
  */
sealed abstract class TransactionalProducerSettings[F[_], K, V] {

  /**
    * The transactional ID which should be used in transactions.
    * This is the value for the following producer property.
    *
    * {{{
    * ProducerConfig.TRANSACTIONAL_ID_CONFIG
    * }}}
    */
  def transactionalId: String

  /**
    * The producer settings including transactional properties,
    * as configured by the [[TransactionalProducerSettings]].
    */
  def producerSettings: ProducerSettings[F, K, V]

  /**
    * Returns a new [[TransactionalProducerSettings]] instance
    * with the specified transaction timeout. This is setting
    * the following producer property, except you can specify
    * it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ProducerConfig.TRANSACTION_TIMEOUT_CONFIG
    * }}}
    */
  def withTransactionTimeout(
    transactionTimeout: FiniteDuration
  ): TransactionalProducerSettings[F, K, V]
}

object TransactionalProducerSettings {
  private[this] final case class TransactionalProducerSettingsImpl[F[_], K, V](
    override val transactionalId: String,
    override val producerSettings: ProducerSettings[F, K, V]
  ) extends TransactionalProducerSettings[F, K, V] {
    override def withTransactionTimeout(
      transactionTimeout: FiniteDuration
    ): TransactionalProducerSettings[F, K, V] =
      copy(
        producerSettings = producerSettings
          .withProperty(
            ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
            transactionTimeout.toMillis.toString
          )
      )

    override def toString: String =
      s"TransactionalProducerSettings(transactionalId = $transactionalId, producerSettings = $producerSettings)"
  }

  def apply[F[_], K, V](
    transactionalId: String,
    producerSettings: ProducerSettings[F, K, V]
  ): TransactionalProducerSettings[F, K, V] =
    TransactionalProducerSettingsImpl(
      transactionalId = transactionalId,
      producerSettings = producerSettings
        .withProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    )

  def unapply[F[_], K, V](
    tps: TransactionalProducerSettings[F, K, V]
  ): Some[(String, ProducerSettings[F, K, V])] =
    Some(
      (
        tps.transactionalId,
        tps.producerSettings
          .withProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, tps.transactionalId)
      )
    )

  implicit def transactionalProducerSettingsShow[F[_], K, V]
    : Show[TransactionalProducerSettings[F, K, V]] =
    Show.fromToString
}
