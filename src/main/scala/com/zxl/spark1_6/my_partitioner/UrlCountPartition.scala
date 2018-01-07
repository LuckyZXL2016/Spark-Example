package com.zxl.spark1_6.my_partitioner

import java.net.URL

import org.apache.spark.{Partitioner, SparkContext, SparkConf}
import scala.collection.mutable

/**
  * 自定义分区
  * 数据格式(时间点  url地址)，例如：
  *   20160321101954	http://net.zxl.cn/net/video.shtml
  * 处理成数据(k, v)
  * 对于数据(k, v)
  * 重写自己的 partitioner
  * Created by ZXL on 2017/10/20.
  */
object UrlCountPartition {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("UrlCountPartition").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // rdd1将数据切分，元组中放的是（URL, 1）
    val rdd1 = sc.textFile("D://test//spark//adv_url_count.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)
    })

    val rdd2 = rdd1.reduceByKey(_ + _)  

    // （URL, n）
    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      // host返回的是如 php.zxl.cn
      (host, (url, t._2))
    })

    // 得到结果为 ArrayBuffer(net.zxl.cn, java.zxl.cn, php.zxl.cn)
    val ints = rdd3.map(_._1).distinct().collect()
//    rdd3.repartition(3).saveAsTextFile("D://test//spark//out//out1")
//    println(ints.toBuffer)

    val hostPartitioner = new HostPartitioner(ints)
    // 取出每个 partitioner 中的信息
    val rdd4 = rdd3.partitionBy(hostPartitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })

    rdd4.saveAsTextFile("D://test//spark//out//out3")

    sc.stop()
  }
}

class HostPartitioner(ins: Array[String]) extends Partitioner {

  val parMap = new mutable.HashMap[String, Int]()
  var count = 0
  for(i <- ins) {
    parMap += (i -> count)
    count += 1
  }

  override def numPartitions: Int = ins.length

  override def getPartition(key: Any): Int = {
    // 根据 key 值获得分区
    parMap.getOrElse(key.toString, 0)
  }
}
