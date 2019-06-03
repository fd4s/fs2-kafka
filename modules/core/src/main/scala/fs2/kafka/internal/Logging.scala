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

package fs2.kafka.internal

import cats.effect.Sync
import cats.implicits._
import org.slf4j.LoggerFactory

private[kafka] sealed abstract class Logging[F[_]] {
  def log(entry: LogEntry): F[Unit]
}

private[kafka] object Logging {
  def default[F[_]](id: Int)(implicit F: Sync[F]): F[Logging[F]] =
    Logging.create[F]("fs2.kafka.KafkaConsumer$" + id)

  def create[F[_]](name: String)(implicit F: Sync[F]): F[Logging[F]] =
    F.delay(LoggerFactory.getLogger(name)).map { logger =>
      new Logging[F] {
        override def log(entry: LogEntry): F[Unit] =
          F.delay {
            entry.level match {
              case LogLevel.Error =>
                if (logger.isErrorEnabled)
                  logger.error(entry.message)
              case LogLevel.Warn =>
                if (logger.isWarnEnabled)
                  logger.warn(entry.message)
              case LogLevel.Info =>
                if (logger.isInfoEnabled)
                  logger.info(entry.message)
              case LogLevel.Debug =>
                if (logger.isDebugEnabled)
                  logger.debug(entry.message)
            }
          }
      }
    }
}
