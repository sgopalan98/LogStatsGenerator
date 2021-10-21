package com.ls

import java.lang.Iterable
import java.util.StringTokenizer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import scala.collection.JavaConverters.*
import com.ls.HelperUtils.Parameters

class LogFileStatsTwo

object LogFileStatsTwo{

  class SecondMapper extends Mapper[Object, Text, Text, Text] {

    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
      //Setting the Time format of the logs
      val format = new java.text.SimpleDateFormat(Parameters.dateFormat)
      //Setting the predefined intervals
      val intervals = Seq((format.parse(Parameters.interval1Start),format.parse(Parameters.interval1End)),
        (format.parse(Parameters.interval2Start), format.parse(Parameters.interval2End)),
        (format.parse(Parameters.interval3Start), format.parse(Parameters.interval3End)))
      //Splitting the text into lines
      val lines = value.toString.split(System.getProperty(Parameters.javaLineSeparator))
      lines.map(line => {
        //Getting the Regex to compare with
        val numPattern = Parameters.regexString.r
        val words = line.split(' ')
        if(words(2) == Parameters.ERROR) {
          //Getting the regex in the line
          val pattern = numPattern.findFirstIn(line)
          pattern match {
            //Checking the pattern
            case Some(regexpattern) => {
              val words = line.split(' ')
              val time = format.parse(words(0))
              //Getting the interval for the timestamp
              val correctInterval = intervals.find(intervals => time.compareTo(intervals._1) >= 0 && intervals._2.compareTo(time) >= 0).get
              word.set(correctInterval._2.toString)
              val output = Text(line)
              //Key - Interval, Value - log line
              context.write(word, output)
            }
            case None => None
          }
        }
      })
    }
  }


  class SecondReducer extends Reducer[Text, Text, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
      //Calculating the sum of the values(no of lines for the interval)
      val sum = values.asScala.size
      context.write(key, new IntWritable(sum))
    }
  }

  class SecondJobMapper extends Mapper[Object, Text, IntWritable, Text] {

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val lines = value.toString.split(System.lineSeparator())
      lines.map(line => {
        //Swapping the key and value because Reducer sorts it on Key;m -1 because we want it in the descending order.
        val k = -1 * line.split(Parameters.lineSeparatorValue)(1).toInt
        val v = line.split(Parameters.lineSeparatorValue)(0)
        context.write(new IntWritable(k), new Text(v))
      })
    }
  }

  class SecondJobReducer extends Reducer[IntWritable, Text, Text, IntWritable] {
    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      //Swapping the key and value again
      val text = values.asScala.map(value => value.toString).toSeq.head
      context.write(new Text(text), new IntWritable(-1*key.get()))
    }
  }


}
