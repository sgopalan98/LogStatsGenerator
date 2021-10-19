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

class LogFileStatsTwo

object LogFileStatsTwo{

  class SecondMapper extends Mapper[Object, Text, Text, Text] {

    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
      val format = new java.text.SimpleDateFormat("HH:mm:ss.SSS")
      val intervals = Seq((format.parse("22:13:49.612"),format.parse("22:13:50.686")),
        (format.parse("22:13:50.686"), format.parse("22:17:54.674")),
        (format.parse("22:17:54.674"), format.parse("22:17:56.043")))
      val lines = value.toString.split(System.getProperty("line.separator"))
      lines.map(line => {
        val numPattern = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}".r
        val words = line.split(' ')
        if(words(2) == "INFO") {
          val pattern = numPattern.findFirstIn(line)
          pattern match {
            case Some(regexpattern) => {
              val words = line.split(' ')
              val time = format.parse(words(0))
              val correctInterval = intervals.find(intervals => time.compareTo(intervals._1) >= 0 && intervals._2.compareTo(time) >= 0).get
              word.set(correctInterval._2.toString)
              val output = Text(line)
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
      val sum = values.asScala.size
      context.write(key, new IntWritable(sum))
    }
  }

}
