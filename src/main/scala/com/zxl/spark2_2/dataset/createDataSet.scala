package com.zxl.spark2_2.dataset

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * DataSet创建的多种方式
  * Created by ZXL on 2018/1/28.
  */
object createDataSet {

  // 构建Spark对象
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("createDataSet")
    .enableHiveSupport()
    .getOrCreate()

  // 导入操作需要的隐式函数
  import spark.implicits._

  // 设置检查点
  spark.sparkContext.setCheckpointDir("hdfs://node1:9000/user/spark_checkpoint")

  // 1.产生序列dataset
  val numDS = spark.range(5, 100, 5)
  numDS.orderBy(desc("id")).show(5)
  numDS.describe().show()

  // 2.集合转成DataSet
  val seq1 = Seq(Person("zxl", 29, 170), Person("wx", 30, 165), Person("cc", 30, 165))
  val ds1 = spark.createDataset(seq1)
  ds1.show()

  // 3.集合转成DataFrame
  val df1 = spark.createDataFrame(seq1).withColumnRenamed("_1", "name").withColumnRenamed("_2", "age")
  df1.orderBy(desc("age")).show(10)

  // 4.rdd转成DataFrame
  val array1 = Array(("zxl", 29, 170), ("wx", 30, 165), ("cc", 30, 165))
  val rdd1 = spark.sparkContext.parallelize(array1, 3).map(f => Row(f._1, f._2, f._3))
  val schema = StructType(
    StructField("name", StringType, false) ::
    StructField("age", IntegerType, true) :: Nil)
  val rddToDataFrame = spark.createDataFrame(rdd1, schema)
  rddToDataFrame.orderBy(desc("name")).show(false)

  // 5.rdd转成DataSet/DataFrame
  val rdd2 = spark.sparkContext.parallelize(array1, 3).map(f => Person(f._1, f._2, f._3))
  val ds2 = rdd2.toDS()
  val df2 = rdd2.toDF()
  ds2.orderBy(desc("name")).show(10)
  df2.orderBy(desc("name")).show(10)

  // 6.rdd转成DataSet
  val ds3 = spark.createDataset(rdd2)
  ds3.show(10)

  // 7.读取文件
  val df4 = spark.read.csv("hdf2://node:9000/test.csv")
  df4.show()

  // 8.读取文件，详细参数
  val schema2 = StructType(
    StructField("name", StringType, false) ::
    StructField("age", IntegerType, false) ::
    StructField("name", IntegerType, true) :: Nil)
  val df7 = spark.read.
    options(Map(("delimiter", ","), ("header", "false"))).
    schema(schema2).csv("hdf2://node:9000/test.csv")
}

case class Person(name: String, age: Int, height: Int)
case class Peoples(age: Int, names: String)