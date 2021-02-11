/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

trait KafkaConsumerLifecycle[F[_]] {

  /**
    * Whenever `terminate` is invoked, an attempt will be made to stop the
    * underlying consumer. The `terminate` operation will not wait for the
    * consumer to shutdown. If you also want to wait for the shutdown
    * to complete, you can use `awaitTermination`.<br>
    */
  def terminate: F[Unit]

  /**
    * Shutdown and wait for it to complete. Note that `awaitTermination` is guaranteed
    * to complete after consumer shutdown, even when the consumer is
    * cancelled with `terminate`.<br>
    */
  def awaitTermination: F[Unit]
}
