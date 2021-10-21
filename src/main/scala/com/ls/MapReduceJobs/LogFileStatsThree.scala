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

class LogFileStatsThree

object LogFileStatsThree{

  class ThirdMapper extends Mapper[Object, Text, Text, Text] {

    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
      val lines = value.toString.split(System.getProperty(Parameters.javaLineSeparator))
      lines.map(line => {
        //Getting the Regex pattern
        val numPattern = Parameters.regexString.r
        //Checking for the pattern in the log file
        val pattern = numPattern.findFirstIn(line)
        pattern match{
          case Some(regexpattern) =>{
            val words = line.split(' ')
            word.set(words(2))
            val output = Text(line)
            //Key - Logtype, Value - Line
            context.write(word,output)
          }
          case None => None
        }
      })
    }
  }


  class ThirdReducer extends Reducer[Text, Text, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
      //Calculating the length of values; No of log messages for the LOGTYPE.
      val sum = values.asScala.size
      context.write(key, new IntWritable(sum))
    }
  }

}
