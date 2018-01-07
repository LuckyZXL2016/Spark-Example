package com.zxl.spark2_2.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming从kafka中读取数据
  * kafka版本0.8
  * 采取直连方式
  *
  * Created by ZXL on 2017/10/15.
  */
object StreamingKafka8 {

  def main(args: Array[String]): Unit = {

    val spark  = SparkSession.builder()
      .master("local[2]")
      .appName("streaming").getOrCreate()

    val sc =spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    // Create direct kafka stream with brokers and topics
    val topicsSet =Set("weblogs")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "node1:9092")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = kafkaStream.map(x => x._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
