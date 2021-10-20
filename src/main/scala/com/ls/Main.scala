package com.ls

import com.ls.LogFileStatsFour.{FourthMapper, FourthReducer}
import com.ls.LogFileStatsOne.{FirstMapper, FirstReducer}
import com.ls.LogFileStatsThree.{ThirdMapper, ThirdReducer}
import com.ls.LogFileStatsTwo.{SecondJobMapper, SecondJobReducer, SecondMapper, SecondReducer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class Main


object Main{

  def main(args: Array[String]): Unit = {

    val programOption = 2
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",")

    if (programOption == 1) {
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
      val job = Job.getInstance(configuration, "Second function")
      job.setJarByClass(LogFileStatsTwo.getClass)
      job.setMapperClass(classOf[SecondMapper])
      job.setReducerClass(classOf[SecondReducer])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path("intermediate.csv"))
      if(!job.waitForCompletion(true))
        System.exit(1)
      val job2 = Job.getInstance(configuration, "Second function part2")
      job2.setJarByClass(LogFileStatsTwo.getClass)
      job2.setMapperClass(classOf[SecondJobMapper])
      job2.setReducerClass(classOf[SecondJobReducer])
      job2.setMapOutputKeyClass(classOf[IntWritable])
      job2.setMapOutputValueClass(classOf[Text])
      job2.setOutputKeyClass(classOf[Text])
      job2.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job2, new Path("intermediate.csv"))
      FileOutputFormat.setOutputPath(job2, new Path(args(1)))
      System.exit(if(job2.waitForCompletion(true)) 0 else 1)
    }

    if(programOption == 3){
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
