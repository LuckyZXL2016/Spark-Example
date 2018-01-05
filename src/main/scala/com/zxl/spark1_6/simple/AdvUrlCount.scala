package com.zxl.spark1_6.simple

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 读取文本内容,根据指定的学科, 取出点击量前三的
  * 文本内容为某广告链接点击量，格式为：
  *     (时间点  某学科url链接)
  *     举例：(20160321101957	http://net.zxl.cn/net/course.shtml)
  * Created by ZXL on 2017/10/16.
  */
object AdvUrlCount {

  def main(args: Array[String]) {

    // 从数据库中加载规则
    val arr = Array("java.zxl.cn", "php.zxl.cn", "net.zxl.cn")

    val conf = new SparkConf().setAppName("AdvUrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // rdd1将数据切分，元组中放的是(URL, 1)
    val rdd1 = sc.textFile("D://test//spark//advUrlCount.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)
    })
    val rdd2 = rdd1.reduceByKey(_ + _)

    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, url, t._2)
    })

    // println(rdd3.collect().toBuffer)

    // val rddjava = rdd3.filter(_._1 == "java.zxl.cn")
    // val sortdjava = rddjava.sortBy(_._3, false).take(3)
    //  val rddphp = rdd3.filter(_._1 == "php.zxl.cn")

    for (ins <- arr) {
      val rdd = rdd3.filter(_._1 == ins)
      val result= rdd.sortBy(_._3, false).take(3)
      //通过JDBC向数据库中存储数据
      //id，学院，URL，次数， 访问日期
      println(result.toBuffer)
    }

    //println(sortdjava.toBuffer)
    sc.stop()
  }
}
