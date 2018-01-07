package com.zxl.spark1_6.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * flume向spark发送数据
  *
  * 添加三个jar包
  *   - commons-lang3-3.3.2.jar
  *   - scala-library-2.10.5.jar
  *   - spark-streaming-flume-sink_2.10-1.6.1.jar
  *
  * 打成jar包上传到集群中运行
  * 集群命令如下：
  * bin/spark-submit --master spark://node1:7077 --class com.zxl.spark1_6.flume.FlumePushWordCount
  *   /jar/____.jar 192.168.13.131 8888
  *
  * Created by ZXL on 2017/10/23.
  */
object FlumePushWordCount {

  def main(args: Array[String]) {
    val host = args(0)
    val port = args(1).toInt
    val conf = new SparkConf().setAppName("FlumeWordCount")//.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //推送方式: flume向spark发送数据
    val flumeStream = FlumeUtils.createStream(ssc, host, port)
    //flume中的数据通过event.getBody()才能拿到真正的内容
    val words = flumeStream.flatMap(x => new String(x.event.getBody().array()).split(" ")).map((_, 1))

    val results = words.reduceByKey(_ + _)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
