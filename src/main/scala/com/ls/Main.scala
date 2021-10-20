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
import com.ls.HelperUtils.Parameters

class Main


object Main{

  def main(args: Array[String]): Unit = {
    
    //Input for which Job to run
    val programOption = args(2).toInt
    val configuration = new Configuration
    configuration.set(Parameters.lineSeperatorKey, Parameters.lineSeparatorValue)

    //For Job one
    if (programOption == 1) {
      val job = Job.getInstance(configuration, Parameters.job0Name)
      //Set the jar for First task
      job.setJarByClass(LogFileStatsOne.getClass)
      //Setting mapper, reducer, input, output constraints
      job.setMapperClass(classOf[FirstMapper])
      job.setReducerClass(classOf[FirstReducer])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path(args(1)))
      System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }

    //For job two
    if (programOption == 2) {
      val job = Job.getInstance(configuration, Parameters.job1Name)
      //Set the jar for Second task
      job.setJarByClass(LogFileStatsTwo.getClass)
      //Setting mapper, reducer, input, output constraints
      job.setMapperClass(classOf[SecondMapper])
      job.setReducerClass(classOf[SecondReducer])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path(Parameters.intermediateFile))
      if(!job.waitForCompletion(true))
        System.exit(1)
      val job2 = Job.getInstance(configuration, Parameters.job1Part2Name)
      job2.setJarByClass(LogFileStatsTwo.getClass)
      job2.setMapperClass(classOf[SecondJobMapper])
      job2.setReducerClass(classOf[SecondJobReducer])
      job2.setMapOutputKeyClass(classOf[IntWritable])
      job2.setMapOutputValueClass(classOf[Text])
      job2.setOutputKeyClass(classOf[Text])
      job2.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job2, new Path(Parameters.intermediateFile))
      FileOutputFormat.setOutputPath(job2, new Path(args(1)))
      System.exit(if(job2.waitForCompletion(true)) 0 else 1)
    }

    //For job three
    if(programOption == 3){
      val job = Job.getInstance(configuration, Parameters.job2Name)
      //Set the jar for third task
      job.setJarByClass(LogFileStatsThree.getClass)
      //Setting mapper, reducer, input, output constraints
      job.setMapperClass(classOf[ThirdMapper])
      job.setReducerClass(classOf[ThirdReducer])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[IntWritable]);
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path(args(1)))
      System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }

    //For job four
    if(programOption == 4){
      val job = Job.getInstance(configuration, Parameters.job3Name)
      ////Set the jar for Fourth task
      job.setJarByClass(LogFileStatsFour.getClass)
      //Setting mapper, reducer, input, output constraints
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
