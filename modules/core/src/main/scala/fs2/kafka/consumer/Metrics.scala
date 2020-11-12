package fs2.kafka.consumer

import org.apache.kafka.common.{MetricName, Metric}

trait Metrics[F[_]] {

  /**
    * Returns consumer metrics.
    *
    * @see org.apache.kafka.clients.consumer.KafkaConsumer#metrics
    */
  def metrics: F[Map[MetricName, Metric]]
}
