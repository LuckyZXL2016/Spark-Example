package com.zxl.spark1_6.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过SparkStreaming简单实现WordCount
  * Created by ZXL on 2017/10/31.
  */
object StreamingWordCount {

  def main(args: Array[String]) {
    // 设置log level
    LoggerLevels.setStreamingLogLevels()

    // StreamingContext
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    // 接收数据，使用nc绑定ip和端口发送数据
    val ds = ssc.socketTextStream("192.168.13.131", 8888)

    // DStream是一个特殊的RDD
    // hello tom hello jerry
    val result = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    // 打印结果
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
