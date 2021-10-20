package com.ls.HelperUtils

import scala.collection.JavaConverters.*
import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}

/*
 *
 *  Copyright (c) 2021. Mark Grechanik and Lone Star Consulting, Inc. All rights reserved.
 *   
 *   Unless required by applicable law or agreed to in writing, software distributed under
 *   the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *   either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 *  
 */

/*
* This module obtains configuration parameter values from application.conf and converts them
* into appropriate scala types.
* */
object Parameters:
  private val logger = CreateLogger(classOf[Parameters.type])
  val config = ObtainConfigReference("logStatsGenerator") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  //Type match is used to dependently type configuration parameter values
  //based on the default input values of the specific config parameter.
  type ConfigType2Process[T] = T match
    case Int => Int
    case Long => Long
    case String => String
    case Double => Double
    case Tuple2[Double, Double] => Tuple2[Double, Double]

  //comparing double values should be done within certain precision
  private val COMPARETHREASHOLD = 0.00001d
  implicit private val comp: Ordering[Double] = new Ordering[Double] {
    def compare(x: Double, y: Double) =
      if math.abs(x - y) <= COMPARETHREASHOLD then 0 else if x - y > COMPARETHREASHOLD then -1 else 1
  }

  //for config parameter likelihood ranges, e.g., error = [0.3, 0.1], they are obtained from the conf file
  //and then sorted in the ascending order
  private def logMsgRange(logTypeName: String): Tuple2[Double, Double] =
    val lst = Try(config.getDoubleList(s"logStatsGenerator.logMessageType.$logTypeName").asScala.toList) match {
      case Success(value) => value.sorted
      case Failure(exception) => logger.error(s"No config parameter is provided: $logTypeName")
        throw new IllegalArgumentException(s"No config data for $logTypeName")
    }
    if lst.length != 2 then throw new IllegalArgumentException(s"Incorrect range of values is specified for log $logTypeName")
    (lst(0), lst(1))
  end logMsgRange

  //It returns a function that takes the name of config entry and obtains the value of this entry if it exists
  //or it logs a warning message if it is absent and returns a default value
  private def func4Parameter[T](defaultVal: T, f: String => T): String => T =
    (pName: String) => Try(f(s"logStatsGenerator.$pName")) match {
      case Success(value) => value
      case Failure(exception) => logger.warn(s"No config parameter $pName is provided. Defaulting to $defaultVal")
        defaultVal
    }
  end func4Parameter

  //in this dependently typed function a typesafe config API method is invoked
  //whose name and return value corresponds to the type of the type parameter, T
  private def getParam[T](pName: String, defaultVal: T): ConfigType2Process[T] =
    defaultVal match {
      case v: Int => func4Parameter(v, config.getInt)(pName)
      case v: Long => func4Parameter(v, config.getLong)(pName)
      case v: String => func4Parameter(v, config.getString)(pName)
      case v: Double => func4Parameter(v, config.getDouble)(pName)
      case v: Tuple2[Double, Double] => logMsgRange(pName)
    }
  end getParam

  import scala.concurrent.duration.*

  val lineSeperatorKey = getParam("LineSeperatorKey","mapred.textoutputformat.separator")

  val lineSeparatorValue = getParam("LineSeperatorValue",",")

  val job0Name = getParam("Job0Name","Job0 - Distribution across time intervals")

  val job1Name = getParam("Job1Name","Job1 - Compute the time interval count")

  val job1Part2Name = getParam("Job1Name","Job1 - Sort the time intervals")

  val job2Name = getParam("Job2Name","Job 2 - Distrubution of log messages")

  val job3Name = getParam("Job3Name", "JOb3 - Compute maximum length in each log type")

  val intermediateFile = getParam("IntermediateFile", "intermediate.csv")

  val dateFormat = getParam("DateFormat", "HH:mm:ss.SSS")
  //Intervals
  val interval1Start = getParam("Interval1Start", "22:13:49.612")
  val interval2Start = getParam("Interval2Start", "22:13:50.686")
  val interval3Start = getParam("Interval3Start", "22:17:54.674")

  val interval1End = getParam("Interval1End", "22:13:50.686")
  val interval2End = getParam("Interval2End", "22:17:54.674")
  val interval3End = getParam("Interval3End", "22:17:56.043")


  val javaLineSeparator = getParam("JavaLineSeparator","line.separator")

  val regexString = getParam("Regex", "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
  //Log types' values
  val INFO = getParam("INFO", "INFO")
  val DEBUG = getParam("DEBUG", "DEBUG")
  val ERROR = getParam("ERROR", "ERROR")
  val WARN = getParam("WARN", "WARN")
