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

class LogFileStatsOne

object LogFileStatsOne{

  class FirstMapper extends Mapper[Object, Text, Text, Text] {

    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
      val format = new java.text.SimpleDateFormat(Parameters.dateFormat)
      val intervals = Seq((format.parse(Parameters.interval1Start),format.parse(Parameters.interval1End)),
        (format.parse(Parameters.interval2Start), format.parse(Parameters.interval2End)),
        (format.parse(Parameters.interval3Start), format.parse(Parameters.interval2End)))
      val lines = value.toString.split(System.getProperty(Parameters.javaLineSeparator))
      lines.map(line => {
        val numPattern = Parameters.regexString.r
        val pattern = numPattern.findFirstIn(line)
        pattern match{
          case Some(regexpattern) =>{
            val words = line.split(' ')
            val time = format.parse(words(0))
            val correctInterval = intervals.find(intervals =>  time.compareTo(intervals._1) >= 0 && intervals._2.compareTo(time) >= 0).get
            word.set(correctInterval._2.toString+" "+words(2))
            val output = Text(line)
            context.write(word,output)
          }
          case None => None
        }
      })
    }
  }


  class FirstReducer extends Reducer[Text, Text, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.size
      context.write(key, new IntWritable(sum))
    }
  }

}
