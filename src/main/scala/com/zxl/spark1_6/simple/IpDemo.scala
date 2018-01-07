package com.zxl.spark1_6.simple

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * 数据格式如下：
  *   (1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302)
  * 根据ip地址转换为数字，从数据集中找出详细信息.
  * 为了简化查找速率，采用二分查找.
  * Created by ZXL on 2017/10/22.
  */
object IpDemo {

  // ip地址转换为数字
  // 如 100.101.102.103，从100开始向左移动8位
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      // | 二进制OR运算符
      // ipNum向左移动8位，相当于乘以256(即2^8)
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  // 从文件中读取数据
  def readData(path: String) = {

    val lines = new ArrayBuffer[String]()

    /**
      * java读取文件方式
      * val br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
      * var s: String = null
      * var flag = true
      * while (flag) {
      * s = br.readLine()
      * if (s != null)
      * lines += s
      * else
      * flag = false
      * }
      * lines
      */

    val content = Source.fromFile(path)
    for (line <- content.getLines()) {
      lines += line
    }
    lines
  }

  // 二分查找ip的下标地址，ip地址已经转为十进制
  def binarySearch(lines: ArrayBuffer[String], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle).split("\\|")(2).toLong) && (ip <= lines(middle).split("\\|")(3).toLong))
        return middle
      if (ip < lines(middle).split("\\|")(2).toLong)
        high = middle - 1
      else
        low = middle + 1
    }
    -1
  }

  def main(args: Array[String]) {
    val ip = "120.55.185.61"
    val ipNum = ip2Long(ip)
    println(ipNum)
    val lines = readData("d://test//spark//ip//ip.txt")
    val index = binarySearch(lines, ipNum)
    print(lines(index))
  }
}
