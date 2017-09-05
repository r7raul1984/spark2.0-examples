package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.{SparkConf, SparkContext}

object SortSample {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("sortsample")
    conf.setMaster("local[4]")
    conf.set("spark.ui.port", "2029")
    val sc = new SparkContext(conf)
    var pairs = sc.parallelize(Array(("e", 0), ("b", 0), ("c", 3), ("d", 6), ("e", 0), ("f", 0), ("g", 3), ("h", 6)), 2)
    var rs = pairs.sortByKey(true, 3)
    rs.collect().foreach(println)
    println(rs.toDebugString)
    Thread.sleep(30000000)
    sc.stop()
  }
}