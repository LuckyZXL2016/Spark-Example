package com.zxl.spark1_6.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 简单连接数据库操作
  * Created by ZXL on 2017/10/22.
  */
object JdbcRDDDemo {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=utf-8", "root", "1234")
    }
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "SELECT * FROM ta where id >= ? AND id <= ?",
      // 1,4分别为两个占位符赋值，2表示两个任务一起读取数据
      1, 4, 2,
      // 返回的内容
      r => {
        val id = r.getInt(1)
        val code = r.getString(2)
        (id, code)
      }
    )
    val data = jdbcRDD.collect()
    println(data.toBuffer)
    sc.stop()
  }
}
