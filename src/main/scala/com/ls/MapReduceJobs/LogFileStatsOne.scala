package com.ls.MapReduceJobs

import com.ls.HelperUtils.Parameters
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import java.util.StringTokenizer
import scala.collection.JavaConverters.*

class LogFileStatsOne

object LogFileStatsOne{

  class FirstMapper extends Mapper[Object, Text, Text, Text] {

    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
      //Setting the date format for the log
      val format = new java.text.SimpleDateFormat(Parameters.dateFormat)
      //Getting the predefined intervals
      val intervals = Seq((format.parse(Parameters.interval1Start),format.parse(Parameters.interval1End)),
        (format.parse(Parameters.interval2Start), format.parse(Parameters.interval2End)),
        (format.parse(Parameters.interval3Start), format.parse(Parameters.interval3End)))
      //Splitting the String into lines
      val lines = value.toString.split(System.getProperty(Parameters.javaLineSeparator))
      lines.map(line => {
        //Regex pattern to be checked for
        val numPattern = Parameters.regexString.r
        val pattern = numPattern.findFirstIn(line)
        pattern match{
          //If pattern matches,
          case Some(regexpattern) =>{
            val words = line.split(' ')
            val time = format.parse(words(0))
            //Getting the interval for the timestamp
            val correctInterval = intervals.find(intervals =>  time.compareTo(intervals._1) >= 0 && intervals._2.compareTo(time) >= 0).get
            word.set(correctInterval._2.toString+" "+words(2))
            //Returning the whole line as the Value
            val output = Text(line)
            //Setting key as INTERVAL-LOGTYPE and Value as Line
            context.write(word,output)
          }
          case None => None
        }
      })
    }
  }


  class FirstReducer extends Reducer[Text, Text, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
      //Getting the size of the values(No of log messages in INterval-LOGTYPE
      val sum = values.asScala.size
      context.write(key, new IntWritable(sum))
    }
  }

}
