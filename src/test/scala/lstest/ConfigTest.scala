package lstest

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.ls.HelperUtils.{ObtainConfigReference, Parameters}

class ConfigTest extends AnyFlatSpec with Matchers {

  behavior of "Configuration getters"

  it should "get the correct interval1Start" in {
    val interval1StartFromParams = Parameters.interval1Start
    val config = ObtainConfigReference("logStatsGenerator") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }
    val interval1StartFromConfig = config.getString("logStatsGenerator.Interval1Start")
    interval1StartFromParams shouldBe interval1StartFromConfig
  }

  it should "get the correct interval1End" in {
    val interval1EndFromParams = Parameters.interval1End
    val config = ObtainConfigReference("logStatsGenerator") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }
    val interval1EndFromConfig = config.getString("logStatsGenerator.Interval1End")
    interval1EndFromParams shouldBe interval1EndFromConfig
  }

  it should "get the correct log types" in {
    val INFOfromParams = Parameters.INFO
    val DEBUGfromParams = Parameters.DEBUG
    val ERRORfromParams = Parameters.ERROR
    val WARNfromParams = Parameters.WARN
    val config = ObtainConfigReference("logStatsGenerator") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }
    val INFOFromConfig = config.getString("logStatsGenerator.INFO")
    val WARNFromConfig = config.getString("logStatsGenerator.WARN")
    val ERRORFromConfig = config.getString("logStatsGenerator.ERROR")
    val DEBUGFromConfig = config.getString("logStatsGenerator.DEBUG")
    INFOFromConfig shouldBe INFOfromParams
    WARNFromConfig shouldBe WARNfromParams
    DEBUGFromConfig shouldBe DEBUGfromParams
    INFOFromConfig shouldBe INFOfromParams
  }

  it should "use the correct Regex" in {
    val regexFromParams = Parameters.regexString

    val config = ObtainConfigReference("logStatsGenerator") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    val regexFromConfig = config.getString("logStatsGenerator.Regex")

    regexFromParams shouldBe regexFromConfig
  }

  it should "use the correct intermediate file" in {
    val intermediateFile = Parameters.intermediateFile

    val config = ObtainConfigReference("logStatsGenerator") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    val iFileFromConfig = config.getString("logStatsGenerator.IntermediateFile")

    intermediateFile shouldBe iFileFromConfig
  }

}
