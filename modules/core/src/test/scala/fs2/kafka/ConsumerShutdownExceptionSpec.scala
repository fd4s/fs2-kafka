package fs2.kafka

final class ConsumerShutdownExceptionSpec extends BaseSpec {
  describe("ConsumerShutdownException") {
    it("should have expected message and toString") {
      val exception = ConsumerShutdownException()
      assert(
        exception.getMessage == "consumer has already shutdown" &&
          exception.toString == "fs2.kafka.ConsumerShutdownException: consumer has already shutdown"
      )
    }
  }
}
