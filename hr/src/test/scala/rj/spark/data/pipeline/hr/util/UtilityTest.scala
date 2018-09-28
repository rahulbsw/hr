package rj.spark.data.pipeline.hr.util

import org.specs2.mutable.Specification
import rj.spark.data.pipeline.hr.util.Utility._

class UtilityTest extends Specification {

  "UtilityTest" should {
    "trimQuotedStr" in {
      //todo
      ok
    }

    "numberCleanup" in {
      (numberCleanup("\"1234.\"").equals("1234"))
    }

    "trimQuotedCSV" in {
      (trimQuotedCSV(
        """"name"
      "age"
      "salary "
      "benefits"
      " department""""
      ).equals("name,age,salary,benefits,department"))
    }

    "numberSplit" in {
      (numberCleanup("\"1234\" \"23  \"").equals(Seq[String]("1234", "23")))
    }

    "trimQuotedCSVString" in {
      ok
    }

    "nvl" in {
      nvl(null, "0").equals("0")
    }

    "booleanCleanup" in {
      (booleanCleanup("\"Yes ,\" 23 \"").equals(true.toString))
    }

    "trimQuotedString" in {
      ok
    }

  }
}
