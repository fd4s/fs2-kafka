package fs2.kafka

final class IsolationLevelSpec extends BaseSpec {
  describe("IsolationLevel") {
    it("should have expected toString") {
      assert(IsolationLevel.ReadCommitted.toString == "ReadCommitted")
      assert(IsolationLevel.ReadUncommitted.toString == "ReadUncommitted")
    }
  }
}
