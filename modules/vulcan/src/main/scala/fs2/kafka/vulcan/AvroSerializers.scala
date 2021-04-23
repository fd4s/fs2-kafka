/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import _root_.vulcan.Codec
import cats.effect.Sync
import fs2.kafka.MkSerializers

final class AvroSerializers[K, V] private[vulcan] (
  implicit val cK: Codec[K],
  cV: Codec[V]
) {
  def using[F[_]](
    settings: AvroSettings[F]
  )(implicit F: Sync[F]): MkSerializers[F, K, V] =
    MkSerializers.instance(
      AvroSerializer.createKey[F, K](settings, cK),
      AvroSerializer.createValue[F, V](settings, cV)
    )

  override def toString: String =
    "AvroSerializers$" + System.identityHashCode(this)
}

object AvroSerializers {
  def apply[K: Codec, V: Codec]: AvroSerializers[K, V] =
    new AvroSerializers
}
