package com.zxl.spark1_6.my_sort

import org.apache.spark.{SparkContext, SparkConf}

// 第二种方式
object OrderContext {

  /**
    * 第一种形式

  implicit object GirlOrdering extends Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue > y.faceValue) 1
      else if(x.faceValue == y.faceValue) {
        if(x.age > y.age) -1 else 1
      } else -1
    }
  }
    */

  /**
    * 第二种形式
    */
  implicit val girlOrdering = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue > y.faceValue) 1
      else if(x.faceValue == y.faceValue) {
        if(x.age > y.age) -1 else 1
      } else -1
    }
  }
}

/**
  * Created by ZXL on 2017/10/21.
  * 自定义排序
  */
object CustomSort {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CustomSort").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("zzz", 90, 28, 1), ("xxx", 90, 27, 2), ("lll", 95, 22, 3)))
    import OrderContext._
    val rdd2 = rdd1.sortBy(x => Girl(x._2, x._3), false)
    println(rdd2.collect().toBuffer)
    sc.stop()
  }
}

/**
  * 第一种方式
  * @param faceValue
  * @param age

case class Girl(val faceValue: Int, val age: Int) extends Ordered[Girl] with Serializable {
  override def compare(that: Girl): Int = {
    if(this.faceValue == that.faceValue) {
      that.age - this.age
    } else {
      this.faceValue - that.faceValue
    }
  }
}
  */

// 第二种方式
case class Girl(faceValue: Int, age: Int) extends Serializable


