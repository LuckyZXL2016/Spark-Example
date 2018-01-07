package com.zxl.spark1_6.kafka

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * 从集群中的kafka读取数据操作
  *
  * 运行时参数：
  *     node1:2181,node2:2181,node3:2181 g1 test 2
  *     其中g1为组名，此处随意写，test为topic名，kafka中的topic名要一致
  *
  * 集群命令(需先启动完成)：
  *     1.启动kafak
  *       bin/kafka-server-start.sh  config/server.properties > /dev/null 2>&1 &
  *     2.创建topic
  *       bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic test
  *     3.向topic中添加数据
  *       bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
  *
  * Created by ZXL on 2017/11/1.
  */
object KafkaWordCount {

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it => Some(it._2.sum + it._3.getOrElse(0)).map(x => (it._1, x)))
    iter.flatMap{case(x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i))}
  }

  def main(args: Array[String]) {

    LoggerLevels.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("D:\\test\\spark\\checkpoint2")
    // 线程执行个数
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    // 返回(K, V)，_._2返回的是值，值得输入是按空格分开
    val words = data.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
