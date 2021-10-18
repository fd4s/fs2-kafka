package fs2.kafka.vulcan

import cats.effect.Async
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.kafka.common.errors.SerializationException

import java.io.IOException

import scala.concurrent.duration._

/**
  * [[SchemaRegistryClientRetry]] describes how to recover from
  * exceptions raised while communicating with a schema registry.
  * See [[SchemaRegistryClientRetry#Default]] for the default
  * recovery strategy. If you do not wish to recover from any
  * exceptions, you can use [[SchemaRegistryClientRetry#None]].<br>
  * <br>
  * To create a new [[SchemaRegistryClientRetry]], simply create a
  * new instance and implement the [[withRetry]] function with the
  * wanted recovery strategy. You can use [[isRetriable]] to
  * identify the exceptions thrown by schema registry failures.
  */
trait SchemaRegistryClientRetry[F[_]] {

  def withRetry[A](action: F[A]): F[A]
}

object SchemaRegistryClientRetry {
  def isRetriable(error: Throwable): Boolean = {
    case _: SerializationException     => true
    case _: IOException                => true
    case apiError: RestClientException => apiError.getErrorCode >= 500
    case _                             => false
  }

  def Default[F[_]](implicit F: Async[F]): SchemaRegistryClientRetry[F] =
    new SchemaRegistryClientRetry[F] {
      override def withRetry[A](action: F[A]): F[A] = {
        def retry(attempt: Int, action: F[A]): F[A] =
          action.handleErrorWith(
            err =>
              if ((attempt + 1) <= 10 && isRetriable(err))
                F.sleep((10 * Math.pow(2, attempt.toDouble)).millis) >> retry(attempt + 1, action)
              else
                F.raiseError(err)
          )

        retry(attempt = 1, action)
      }

      override def toString: String = "RetryPolicy.Default"
    }

  def None[F[_]]: SchemaRegistryClientRetry[F] =
    new SchemaRegistryClientRetry[F] {
      override def withRetry[A](action: F[A]): F[A] = action

      override def toString: String = "RetryPolicy.None"
    }
}
