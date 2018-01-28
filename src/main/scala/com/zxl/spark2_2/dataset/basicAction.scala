package com.zxl.spark2_2.dataset

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._

/**
  * DataSet的基本操作
  * Created by ZXL on 2018/1/28.
  */
object basicAction {

  // 构建Spark对象
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("createDataSet")
    .enableHiveSupport()
    .getOrCreate()

  // 导入操作需要的隐式函数
  import spark.implicits._

  // 1.DataSet存储类型
  val seq1 = Seq(Person("zxl", 29, 170), Person("wx", 30, 165), Person("cc", 30, 165))
  val ds1 = spark.createDataset(seq1)
  ds1.show()
  ds1.checkpoint()
  ds1.cache()
  ds1.persist(MEMORY_ONLY)
  ds1.count()
  ds1.show()
  ds1.unpersist(true)   // 将DataSet删除

  // 2.获取数据集
  val c1 = ds1.collect()
  val c2 = ds1.collectAsList()
  val h1 = ds1.head()
  val h2 = ds1.head(3)
  val f1 = ds1.first()
  val t1 = ds1.take(2)
  val t2 = ds1.takeAsList(2)

  // 3.统计数据集
  ds1.count()
  ds1.describe().show()
  ds1.describe("age").show()
  ds1.describe("age", "height").show()

  // 4.聚集
  ds1.reduce((f1, f2) => Person("sum", (f1.age + f2.age), (f1.height + f2.height)))

  // 5.DataSet结构属性
  ds1.columns
  ds1.dtypes
  ds1.explain()   // 返回执行物理计划

  // 6.DataSet rdd数据互转
  val rdd1 = ds1.rdd
  val ds2 = rdd1.toDS()
  ds2.show()
  val df2 = rdd1.toDF()
  df2.show()

  // 7.DataSet 保存文件
  ds1.select("name", "age", "height").write.format("csv").save("hdfs://node1:9000/test2.csv")
  // 读取保存的文件
  val schema2 = StructType(
    StructField("name", StringType, false) ::
      StructField("age", IntegerType, false) ::
      StructField("name", IntegerType, true) :: Nil)
  val out = spark.read.
    options(Map(("delimiter", ","), ("header", "false"))).
    schema(schema2).csv("hdf2://node:9000/test2.csv")
  out.show(10)
}
