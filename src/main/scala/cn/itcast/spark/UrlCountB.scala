package cn.itcast.spark

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

object UrlCountB {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCountB").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("/Users/pier/Desktop/work/传智/文档资料/day29/itcast.log").map(x => {
      val arr = x.split("\t")
      (arr(1), 1)
    })

    val rdd2 = rdd1.reduceByKey(_+_)

    val rdd3 = rdd2.map(it => {
      val url = it._1
      val host = new URL(url).getHost()
      val count = it._2
      (host, url, count)
    })


    val rdd4 = rdd3.groupBy(_._1).mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(3)
    })

    println(rdd4.collect().toBuffer)

    sc.stop()
  }
}
