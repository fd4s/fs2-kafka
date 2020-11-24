/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import cats.effect.Fiber

trait ConsumerFiber[F[_]] {

  /**
    * A `Fiber` that can be used to cancel the underlying consumer, or
    * wait for it to complete. If you're using [[stream]], or any other
    * provided stream in [[KafkaConsumer]], these will be automatically
    * interrupted when the underlying consumer has been cancelled or
    * when it finishes with an exception.<br>
    * <br>
    * Whenever `cancel` is invoked, an attempt will be made to stop the
    * underlying consumer. The `cancel` operation will not wait for the
    * consumer to shutdown. If you also want to wait for the shutdown
    * to complete, you can use `join`. Note that `join` is guaranteed
    * to complete after consumer shutdown, even when the consumer is
    * cancelled with `cancel`.<br>
    * <br>
    * This `Fiber` instance is usually only required if the consumer
    * needs to be cancelled due to some external condition, or when an
    * external process needs to be cancelled whenever the consumer has
    * shut down. Most of the time, when you're only using the streams
    * provided by [[KafkaConsumer]], there is no need to use this.
    */
  def fiber: Fiber[F, Unit]
}
