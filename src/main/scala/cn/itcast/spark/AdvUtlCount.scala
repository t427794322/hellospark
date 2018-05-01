package cn.itcast.spark

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

object AdvUtlCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AdvUtlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val hostArr = Array("net.itcast.cn", "java.itcast.cn", "php.itcast.cn")

    val rdd1 = sc.textFile("/Users/pier/Desktop/work/传智/文档资料/day29/itcast.log").map(x => {
      val tempArr = x.split("\t")
      (tempArr(1), 1)
    })

    val rdd2 = rdd1.reduceByKey(_+_)
    println(rdd2.collect().toBuffer)

    val rdd3 = rdd2.map(x => {
      val url = x._1
      val host = new URL(url).getHost
      val count = x._2
      (host, url, count)
    })

    println("============")
    println(rdd3.collect().toBuffer)

    for (tempHost <- hostArr) {
//      val tempStat = rdd3.filter(_._1 eq tempHost)
      val tempStat = rdd3.filter(_._1 equals tempHost)
      val result = tempStat.sortBy(_._3, false).take(3)
      println(result.toBuffer)
    }

    sc.stop()
  }
}
