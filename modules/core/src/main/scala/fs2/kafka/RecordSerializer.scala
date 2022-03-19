package fs2.kafka

sealed trait RecordSerializer[F[_], K, V] {
  def forKey: KeySerializer[F, K]
  def forValue: ValueSerializer[F, V]
}

object RecordSerializer {
  implicit def instance[F[_], K, V](
    implicit k: KeySerializer[F, K],
    v: ValueSerializer[F, V]
  ): RecordSerializer[F, K, V] = new RecordSerializer[F, K, V] {
    override def forKey: KeySerializer[F, K] = k

    override def forValue: ValueSerializer[F, V] = v
  }
}
