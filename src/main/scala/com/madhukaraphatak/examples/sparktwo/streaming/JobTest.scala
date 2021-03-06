package com.madhukaraphatak.examples.sparktwo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, TestInputStream}

object JobTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local[2]")
    conf.set("spark.streaming.concurrentJobs", "2")
    val sc = new StreamingContext(conf, Seconds(10))

    val input = new TestInputStream[String](sc, Seq(Seq("1", "2", "3"), Seq("1", "2", "3"), Seq("1", "2", "3")), 2)
    val input2 = new TestInputStream[String](sc, Seq(Seq("1", "2", "3"), Seq("1", "2", "3"), Seq("1", "2", "3")), 2)

    input.map{f=>
      Thread.sleep(5000)
      f
    }.foreachRDD(f=>f.count())

    input2.map{f=>
      Thread.sleep(5000)
      f
    }.foreachRDD(f=>f.count())

    sc.start()
    sc.awaitTermination()

  }
}
