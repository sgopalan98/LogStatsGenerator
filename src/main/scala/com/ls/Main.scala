package com.ls

import com.ls.LogFileStatsFour.{FourthMapper, FourthReducer}
import com.ls.LogFileStatsOne.{FirstMapper, FirstReducer}
import com.ls.LogFileStatsThree.{ThirdMapper, ThirdReducer}
import com.ls.LogFileStatsTwo.{SecondMapper, SecondReducer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class Main


object Main{

  def main(args: Array[String]): Unit = {

    val programOption = 4

    if (programOption == 1) {
      val configuration = new Configuration
      val job = Job.getInstance(configuration, "First function")
      job.setJarByClass(LogFileStatsOne.getClass)
      job.setMapperClass(classOf[FirstMapper])
      job.setReducerClass(classOf[FirstReducer])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path(args(1)))
      System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }

    if (programOption == 2) {
      val configuration = new Configuration
      val job = Job.getInstance(configuration, "Second function")
      job.setJarByClass(LogFileStatsTwo.getClass)
      job.setMapperClass(classOf[SecondMapper])
      job.setReducerClass(classOf[SecondReducer])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path(args(1)))
      System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }

    if(programOption == 3){
      val configuration = new Configuration
      val job = Job.getInstance(configuration, "Third function")
      job.setJarByClass(LogFileStatsThree.getClass)
      job.setMapperClass(classOf[ThirdMapper])
      job.setReducerClass(classOf[ThirdReducer])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[IntWritable]);
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path(args(1)))
      System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }

    if(programOption == 4){
      val configuration = new Configuration
      val job = Job.getInstance(configuration, "Fourth function")
      job.setJarByClass(LogFileStatsFour.getClass)
      job.setMapperClass(classOf[FourthMapper])
      job.setReducerClass(classOf[FourthReducer])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[IntWritable]);
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path(args(1)))
      System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }
  }

}
