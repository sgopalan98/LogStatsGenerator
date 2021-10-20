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

class LogFileStatsFour

object LogFileStatsFour{

  class FourthMapper extends Mapper[Object, Text, Text, IntWritable] {

    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val lines = value.toString.split(System.getProperty("line.separator"))
      lines.map(line => {
        val numPattern = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}".r
        val pattern = numPattern.findFirstIn(line)
        pattern match{
          case Some(regexpattern) =>{
            val words = line.split(' ')
            word.set(words(2))
            if(words(2) == "INFO" || words(2) == "WARN") {
              val logMessage: String = words(6)
              val output = IntWritable(logMessage.length())
              context.write(word, output)
            }
            else{
              val logMessage: String = words(5)
              val output = IntWritable(logMessage.length())
              context.write(word, output)
            }
          }
          case None => None
        }
      })
    }
  }


  class FourthReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val valuesList: List[Int] = values.asScala.map(value => {
        value.get
      }).toList
      val maxValue = valuesList.max
      context.write(key, new IntWritable(maxValue))
    }
  }

}
