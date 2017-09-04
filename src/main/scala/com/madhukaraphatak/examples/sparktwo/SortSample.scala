package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SortSample {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("sortsample")
    conf.setAppName("test")
    conf.setMaster("local[4]")
    conf.set("spark.ui.port","2029")
    val sc = new SparkContext(conf)
    var pairs = sc.parallelize(Array(("e",0),("b",0),("c",3),("d",6),("e",0),("f",0),("g",3),("h",6)), 2);
    pairs.sortByKey(true, 3).collect().foreach(println);
  }
}