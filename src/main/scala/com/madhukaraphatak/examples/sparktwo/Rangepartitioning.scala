package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
//https://issues.apache.org/jira/browse/SPARK-17788
//https://issues.apache.org/jira/browse/SPARK-9862  Join: Handling data skew
//http://www.jasongj.com/spark/skew/
object Rangepartitioning {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sortsample")
    conf.setMaster("local[4]")
    conf.set("spark.ui.port", "2029")
    // Set the partitions and parallelism to relatively low value so we can read the results.
    conf.set("spark.default.parallelism", "20")
    conf.set("spark.sql.shuffle.partitions", "20")
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    // Create a skewed data frame.
    val df = spark
      .range(10000000)
      .select(
        $"id",
        (rand(34) * when($"id" % 10 <= 7, lit(1.0)).otherwise(lit(10.0))).as("value"))

    // Make a summary per partition. The partition intervals should not overlap and the number of
    // elements in a partition should roughly be the same for all partitions.
    case class PartitionSummary(count: Long, min: Double, max: Double, range: Double)
    val res = df.orderBy($"value").mapPartitions { iterator =>
      val (count, min, max) = iterator.foldLeft((0L, Double.PositiveInfinity, Double.NegativeInfinity)) {
        case ((count, min, max), Row(_, value: Double)) =>
          (count + 1L, Math.min(min, value), Math.max(max, value))
      }
      Iterator.single(PartitionSummary(count, min, max, max - min))
    }

    // Get results and make them look nice
    res.orderBy($"min")
      .select($"count", $"min".cast("decimal(5,3)"), $"max".cast("decimal(5,3)"), $"range".cast("decimal(5,3)"))
      .show(30)
  }

}
