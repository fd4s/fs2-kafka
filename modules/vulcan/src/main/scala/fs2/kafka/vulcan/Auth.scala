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

package fs2.kafka.vulcan

/**
  * The available options for [[SchemaRegistryClientSettings#withAuth]].
  *
  * Available options include:<br>
  * - [[Auth.Basic]] to authenticate with username and password,<br>
  * - [[Auth.Bearer]] to authenticate with an authentication token,<br>
  * - [[Auth.None]] to not provide any authentication details.
  */
sealed abstract class Auth

object Auth {
  private[vulcan] final case class BasicAuth(username: String, password: String) extends Auth {
    override def toString: String = s"Basic($username)"
  }

  private[vulcan] final case class BearerAuth(token: String) extends Auth {
    override def toString: String = "Bearer"
  }

  private[vulcan] case object NoAuth extends Auth {
    override def toString: String = "None"
  }

  /**
    * Option to authenticate with username and password.
    */
  def Basic(username: String, password: String): Auth =
    BasicAuth(username, password)

  /**
    * Option to authenticate with an authentication token.
    */
  def Bearer(token: String): Auth =
    BearerAuth(token)

  /**
    * Option to not provide any authentication details.
    */
  val None: Auth =
    NoAuth
}
