package cn.itcast.spark.day3

import java.net.URL

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object UrlCountPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCountPartition").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("/Users/pier/Desktop/work/传智/文档资料/day29/itcast.log").map(line => {
      val tempArr = line.split("\t")
      (tempArr(1), 1)
    })

    val rdd2 = rdd1.reduceByKey(_+_)

    val rdd3 = rdd2.map(x => {
      val url = x._1
      val host = new URL(url).getHost
      val count = x._2
      (host, (url, count))
    }).cache()

    val hostsArr = rdd3.map(_._1).distinct().collect()
    println("hostsArr=" + hostsArr.toBuffer)

    val rdd4 = rdd3.partitionBy(new HashPartitioner(hostsArr.length))

//    val hostPartitioner = new HostPartitioner(hostsArr)
//    val rdd4 = rdd3.partitionBy(hostPartitioner).mapPartitions(iter => {
//      iter.toList.sortBy(_._2._2).reverse.take(2).iterator
//    })

    val ts = System.currentTimeMillis()
    rdd4.saveAsTextFile("/Users/pier/Desktop/work/test/urlResult" + ts + ".txt")

    sc.stop()
  }
}

class HostPartitioner(ins: Array[String]) extends Partitioner {

  val parMap = new mutable.HashMap[String, Int]()

  var count: Int = 0
  for (tempPos <- ins) {
    parMap += (tempPos -> count)
    count += 1
  }

  override def numPartitions: Int = ins.length

  override def getPartition(key: Any): Int = {
    parMap.getOrElse(key.toString, 0)
  }
}
