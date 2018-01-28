package com.zxl.spark2_2.dataset

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * DataSet的操作
  * Created by ZXL on 2018/1/28.
  */
object actions {

  // 构建Spark对象
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("createDataSet")
    .enableHiveSupport()
    .getOrCreate()

  // 导入操作需要的隐式函数
  import spark.implicits._

  // 1.map操作，flatMap操作
  val seq1 = Seq(Peoples(21, "zxl,wr,hy"), Peoples(20, "cc,hw,lwq"))
  val ds1 = spark.createDataset(seq1)
  val ds2 = ds1.map{ x => (x.age + 1, x.names)}.show()
  val ds3 = ds1.flatMap{ x =>
    val a = x.age
    val s = x.names.split(",").map{ x => (a, x)}
    s
  }.show()

  // 2.filter操作，where操作
  val seq2 = Seq(Person("zxl", 29, 170), Person("wx", 30, 165), Person("cc", 30, 165))
  val ds4 = spark.createDataset(seq2)
  ds4.filter("age >= 20 and height >= 170").show()
  ds4.filter($"age" >= 20 && $"height" >= 170).show()
  ds4.filter{x => x.age > 20 && x.height >= 170}.show()
  ds4.where("age >= 20 and height >= 170").show()
  ds4.where($"age" >= 20 && $"height" >= 170).show()

  // 3.去重操作
  ds4.distinct().show()
  ds4.dropDuplicates("age").show()
  ds4.dropDuplicates("age", "height").show()
  ds4.dropDuplicates(Seq("age", "height")).show()
  ds4.dropDuplicates(Array("age", "height")).show()

  // 4.加法/减法操作
  val seq3 = Seq(Person("zxl2", 29, 170), Person("wx2", 30, 165), Person("cc2", 30, 165))
  val ds5 = spark.createDataset(seq3)
  ds4.except(ds5).show()
  ds4.union(ds5).show()
  ds4.intersect(ds5).show()

  // 5.select操作
  ds5.select("name", "age").show()
  ds5.select(expr("height + 1").as[Int]).show()

  // 6.排序操作
  ds5.sort("age").show()
  ds5.sort($"age".desc, $"height".desc).show()
  ds5.orderBy("age").show()
  ds5.orderBy($"age".desc, $"height".desc).show()

  // 7.分割抽样操作
  val ds6 = ds4.union(ds5)
  val rands = ds6.randomSplit(Array(0.3, 0.7))
  rands(0).count()
  rands(1).count()
  rands(0).show()
  rands(1).show()
  val ds7 = ds6.sample(false, 0.5)
  ds7.count()
  ds7.show()

  // 8.列操作
  val ds8 = ds6.drop("height")
  ds8.columns
  ds8.show()
  val ds9 = ds6.withColumn("add2", $"age" + 2)    // 对数据集增加列
  ds9.columns
  ds9.show()
  val ds10 = ds9.withColumnRenamed("add2", "age_new")
  ds10.columns
  ds10.show()
  ds6.withColumn("add_col", lit(1)).show()

  // 9.join操作
  val seq4 = Seq(Score("zxl", 85), Score("wr", 90), Score("hy", 95))
  val ds11 = spark.createDataset(seq4)
  val ds12 = ds5.join(ds11, Seq("name"), "inner")
  ds12.show()
  val ds13 = ds5.join(ds11, Seq("name"), "left")
  ds13.show()

  // 10.分组聚合操作
  val ds14 = ds4.union(ds5).groupBy("height").agg(avg("age")).as("avg_agg")
  ds14.show()
}

case class Score(name: String, score: Int)