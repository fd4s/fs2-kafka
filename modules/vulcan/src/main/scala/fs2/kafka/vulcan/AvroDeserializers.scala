/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import _root_.vulcan.Codec
import cats.effect.Sync
import fs2.kafka.MkDeserializers

final class AvroDeserializers[K, V] private[vulcan] (
  implicit cK: Codec[K],
  cV: Codec[V]
) {
  def using[F[_]](
    settings: AvroSettings[F]
  )(implicit F: Sync[F]): MkDeserializers[F, K, V] =
    MkDeserializers.instance(
      AvroDeserializer.createKey[F, K](settings, cK),
      AvroDeserializer.createValue[F, V](settings, cV)
    )

  override def toString: String =
    "AvroDeserializers$" + System.identityHashCode(this)
}

object AvroDeserializers {
  def apply[K: Codec, V: Codec]: AvroDeserializers[K, V] =
    new AvroDeserializers
}
