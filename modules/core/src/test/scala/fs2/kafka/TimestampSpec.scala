package fs2.kafka

import cats.syntax.show._

final class TimestampSpec extends BaseSpec {
  describe("Timeout#createTime") {
    it("should have a createTime") {
      forAll { value: Long =>
        val timestamp = Timestamp.createTime(value)
        timestamp.createTime shouldBe Some(value)
        timestamp.nonEmpty shouldBe true
        timestamp.isEmpty shouldBe false
      }
    }

    it("should not have a logAppendTime") {
      forAll { value: Long =>
        Timestamp.createTime(value).logAppendTime shouldBe None
      }
    }

    it("should not have an unknownTime") {
      forAll { value: Long =>
        Timestamp.createTime(value).unknownTime shouldBe None
      }
    }

    it("should include the createTime in toString") {
      forAll { value: Long =>
        val timestamp = Timestamp.createTime(value)
        timestamp.toString should include(s"createTime = $value")
        timestamp.toString shouldBe timestamp.show
      }
    }
  }

  describe("Timeout#logAppendTime") {
    it("should have a logAppendTime") {
      forAll { value: Long =>
        val timestamp = Timestamp.logAppendTime(value)
        timestamp.logAppendTime shouldBe Some(value)
        timestamp.nonEmpty shouldBe true
        timestamp.isEmpty shouldBe false
      }
    }

    it("should not have a createTime") {
      forAll { value: Long =>
        Timestamp.logAppendTime(value).createTime shouldBe None
      }
    }

    it("should not have an unknownTime") {
      forAll { value: Long =>
        Timestamp.logAppendTime(value).unknownTime shouldBe None
      }
    }

    it("should include the logAppendTime in toString") {
      forAll { value: Long =>
        val timestamp = Timestamp.logAppendTime(value)
        timestamp.toString should include(s"logAppendTime = $value")
        timestamp.toString shouldBe timestamp.show
      }
    }
  }

  describe("Timestamp#none") {
    it("should not have createTime") {
      Timestamp.none.createTime shouldBe None
    }

    it("should not have logAppendTime") {
      Timestamp.none.logAppendTime shouldBe None
    }

    it("should not have unknownTime") {
      Timestamp.none.unknownTime shouldBe None
    }

    it("should be empty") {
      Timestamp.none.isEmpty shouldBe true
      Timestamp.none.nonEmpty shouldBe false
    }

    it("should not include any time in toString") {
      Timestamp.none.toString shouldBe "Timestamp()"
      Timestamp.none.show shouldBe Timestamp.none.toString
    }
  }

  describe("Timeout#unknownTime") {
    import org.scalacheck.Arbitrary
    import org.scalacheck.Arbitrary.arbitrary
    import org.apache.kafka.clients.consumer.ConsumerRecord.NO_TIMESTAMP

    implicit val abnormalTimestamp: Arbitrary[Long] =
      Arbitrary(arbitrary[Long].filterNot(_ == NO_TIMESTAMP))

    it("should have an unknownTime when timestamp is other than NO_TIMESTAMP") {
      forAll { value: Long =>
        val timestamp = Timestamp.unknownTime(value)
        timestamp.unknownTime shouldBe Some(value)
        timestamp.nonEmpty shouldBe false
        timestamp.isEmpty shouldBe true
      }
    }

    it("should not have an unknownTime when timestamp is NO_TIMESTAMP") {
      val timestamp = Timestamp.unknownTime(NO_TIMESTAMP)
      timestamp.unknownTime shouldBe None
      timestamp.nonEmpty shouldBe false
      timestamp.isEmpty shouldBe true
    }

    it("should not have a createTime") {
      forAll { value: Long =>
        Timestamp.unknownTime(value).createTime shouldBe None
      }
    }

    it("should not have a logAppendTime") {
      forAll { value: Long =>
        Timestamp.unknownTime(value).logAppendTime shouldBe None
      }
    }

    it("should include the unknownTime in toString") {
      forAll { value: Long =>
        val timestamp = Timestamp.unknownTime(value)
        timestamp.toString should include(s"unknownTime = $value")
        timestamp.toString shouldBe timestamp.show
      }
    }
  }
}
