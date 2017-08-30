package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.{SparkConf, SparkContext}

object StageExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    val input1 = sc.makeRDD(Seq(("a", 1), ("b", 1)), 2).map(f => {
      Thread.sleep(3000)
      f
    })
    val input2 = sc.makeRDD(Seq(("a", 1), ("b", 1), ("b", 1), ("b", 1), ("b", 1), ("b", 1)), 4).map(f => {
      Thread.sleep(5000)
      f
    }).reduceByKey((a, b) => {
      Thread.sleep(1000)
      a + b
    })
    val input3 = input1.join(input2, 1)
    input3.count()
    Thread.sleep(30000000)
    sc.stop()
  }
}
