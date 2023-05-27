/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import org.apache.kafka.common.{MetricName, Metric}

trait KafkaMetrics[F[_]] {

  /** Returns consumer metrics.
    *
    * @see org.apache.kafka.clients.consumer.KafkaConsumer#metrics
    */
  def metrics: F[Map[MetricName, Metric]]
}
