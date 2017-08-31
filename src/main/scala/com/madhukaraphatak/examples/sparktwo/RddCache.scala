package com.madhukaraphatak.examples.sparktwo
//https://stackoverflow.com/questions/35127720/what-is-the-difference-between-spark-checkpoint-and-persist-to-a-disk
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object RddCache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local[4]")
    conf.set("spark.ui.port","2029")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("D:\\check")
    val rdd = sc.parallelize(1 to 10).map(x => (x % 3, 1)).reduceByKey(_ + _,2)
    val indCache  = rdd.mapValues(_ > 4)
    //indCache.checkpoint
    indCache.count
   //indCache.persist(StorageLevel.DISK_ONLY_2)
    indCache.cache()

     // shuffle_0_0_0.data shuffle_0_3_0.index  C:\Users\tangjijun\AppData\Local\Temp\blockmgr-bb3a6ddf-5351-4dc5-b8be-24e938ff3ef5\0f
    println(indCache.toDebugString)
    // (8) MapPartitionsRDD[13] at mapValues at <console>:24 [Disk Serialized 1x Replicated]
    //  |  ShuffledRDD[3] at reduceByKey at <console>:21 [Disk Serialized 1x Replicated]
    //  +-(8) MapPartitionsRDD[2] at map at <console>:21 [Disk Serialized 1x Replicated]
    //     |  ParallelCollectionRDD[1] at parallelize at <console>:21 [Disk Serialized 1x Replicated]

    indCache.count
    // 3

    println(indCache.toDebugString)
    // (8) MapPartitionsRDD[13] at mapValues at <console>:24 [Disk Serialized 1x Replicated]
    //  |       CachedPartitions: 8; MemorySize: 0.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 587.0 B
    //  |  ShuffledRDD[3] at reduceByKey at <console>:21 [Disk Serialized 1x Replicated]
    //  +-(8) MapPartitionsRDD[2] at map at <console>:21 [Disk Serialized 1x Replicated]
    //     |  ParallelCollectionRDD[1] at parallelize at <console>:21 [Disk Serialized 1x Replicated]


   // indChk.checkpoint

    // indChk.toDebugString
    // (8) MapPartitionsRDD[11] at mapValues at <console>:24 []
    //  |  ShuffledRDD[3] at reduceByKey at <console>:21 []
    //  +-(8) MapPartitionsRDD[2] at map at <console>:21 []
    //     |  ParallelCollectionRDD[1] at parallelize at <console>:21 []

    //indChk.count
    // 3

    //indChk.toDebugString
    // (8) MapPartitionsRDD[11] at mapValues at <console>:24 []
    //  |  ReliableCheckpointRDD[12] at count at <console>:27 []
    Thread.sleep(30000000)
    sc.stop()
  }
}
