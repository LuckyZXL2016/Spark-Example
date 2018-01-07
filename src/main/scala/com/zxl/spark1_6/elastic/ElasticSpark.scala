package com.zxl.spark1_6.elastic

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Elasticsearch是一个基于Lucene的实时地分布式搜索和分析引擎。
  * 设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。
  *
  * Created by ZXL on 2017/10/23.
  */
object ElasticSpark {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ElasticSpark").setMaster("local")
    conf.set("es.nodes", "192.168.13.131,192.168.13.132,192.168.13.133")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)
    //val query: String = "{\"query\":{\"match_all\":{}}}"
    val start = 1463998397
    val end = 1463998399
//    val query: String =
//      s"""{
//       "query": {"match_all": {}},
//       "filter": {
//         "bool": {
//           "must": {
//             "range": {
//               "access.time": {
//                 "gte": "$start",
//                 "lte": "$end"
//               }
//             }
//           }
//         }
//       }
//     }"""

    val tp = "1"
    val query: String = s"""{
       "query": {"match_all": {}},
       "filter" : {
          "bool": {
            "must": [
                {"term" : {"access.type" : $tp}},
                {
                "range": {
                  "access.time": {
                  "gte": "$start",
                  "lte": "$end"
                  }
                }
              }
            ]
          }
       }
     }"""
    val rdd1 = sc.esRDD("accesslogs", query)

    println(rdd1.collect().toBuffer)
    println(rdd1.collect().size)
  }
}
