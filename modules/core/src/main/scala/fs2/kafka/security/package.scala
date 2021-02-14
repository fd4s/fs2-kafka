/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.annotation.tailrec

package object security {
  private[this] final val hexChars: Array[Char] =
    Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

  implicit class StringOps(val str: String) extends AnyVal {
    private[this] final def hex(in: Array[Byte]): Array[Char] = {
      val length = in.length

      @tailrec def encode(out: Array[Char], i: Int, j: Int): Array[Char] =
        if (i < length) {
          out(j) = hexChars((0xf0 & in(i)) >>> 4)
          out(j + 1) = hexChars(0x0f & in(i))
          encode(out, i + 1, j + 2)
        } else out

      encode(new Array(length << 1), 0, 0)
    }

    private[this] final def sha1(bytes: Array[Byte]): Array[Byte] =
      MessageDigest.getInstance("SHA-1").digest(bytes)

    def sha1Hex: String =
      new String(hex(sha1(str.getBytes(StandardCharsets.UTF_8))))

    final def valueShortHash: String =
      sha1Hex.take(7)
  }
}
