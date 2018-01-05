package com.zxl.spark1_6.simple

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 简单WordCount实现
  * Created by ZXL on 2017/10/12.
  *
  * 集群上执行示例，指定相关配置
  * bin/spark-submit --master spark://node1:7077 --class com.zxl.spark1_6.simple.WordCount --executor-memory 512m
  * --total-executor-cores 2 /opt/soft/jar/hello-spark-1.0.jar hdfs://node1:9000/wc hdfs://node1:9000/out
  */
object WordCount {

  def main(args: Array[String]) {
    // 非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    // reduceByKey(_+_, 1)指定partition的个数为1，即生成一个输出文件
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).sortBy(_._2, false).saveAsTextFile(args(1))
    sc.stop()
  }
}
