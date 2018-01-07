package com.zxl.spark1_6.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 从hdfs中读取数据，转化为DataFrame，执行简单操作
  * Created by ZXL on 2017/10/23.
  */
object SQLDemo {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SQLDemo")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // 设置可以读取集群中的hdfs中文件
    System.setProperty("user.name", "root")

    val personRdd = sc.textFile("hdfs://node1:9000/person.txt").map(line =>{
      val fields = line.split(",")
      Person(fields(0).toLong, fields(1), fields(2).toInt)
    })

    import sqlContext.implicits._
    // 转为DataFrame
    val personDf = personRdd.toDF

    personDf.show()

    personDf.registerTempTable("person")

    sqlContext.sql("select * from person where age >= 20 order by age desc limit 2").show()

    sc.stop()

  }

  case class Person(id: Long, name: String, age: Int)
}
