package com.zxl.spark1_6.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/**
  * SparkStreaming窗口函数的实现
  * Created by ZXL on 2017/11/2.
  */
object WindowOpts {

  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("WindowOpts").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Milliseconds(5000))
    val lines = ssc.socketTextStream("192.168.13.131", 9999)
    val pairs = lines.flatMap(_.split(" ")).map((_, 1))
    // Seconds(15)：窗口的宽度，Seconds(10)：移动窗口的间隔
    val windowedWordCounts = pairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(15), Seconds(10))
    windowedWordCounts.print()
    // Map((hello, 5), (jerry, 2), (kitty, 3))
    val a = windowedWordCounts.map(_._2).reduce(_+_)
    a.foreachRDD(rdd => {
      println(rdd.take(0))
    })
    a.print()

    // windowedWordCounts.map(t => (t._1, t._2.toDouble / a.toD))
    ssc.start()
    ssc.awaitTermination()
  }
}
