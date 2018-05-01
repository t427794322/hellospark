package cn.itcast.spark.day3

import org.apache.spark.{SparkConf, SparkContext}

object IPLocation {

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines: Array[(String, String, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IPLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ipRdds = sc.textFile("/Users/pier/Desktop/work/传智/文档资料/day30/计算IP地址归属地/ip.txt")
          .map(line => {
            val ipInfoArr = line.split("\\|")
            val startIpNum = ipInfoArr(2)
            val endIpNum = ipInfoArr(3)
            val province = ipInfoArr(6)
            (startIpNum, endIpNum, province)
          })

    val ipInfoArr = ipRdds.collect()

    val ipInfoBroadcast = sc.broadcast(ipInfoArr)

    val result = sc.textFile("/Users/pier/Desktop/work/test/testdata/access_log.txt").map(line => {
      val tempArr = line.split(" ")
      val tempIp = ip2Long(tempArr(0))
      val ipIdx = binarySearch(ipInfoBroadcast.value, tempIp)
      if (-1 != ipIdx) {
        val ipInfo = ipInfoBroadcast.value(ipIdx)
        ipInfo
      } else {
        None
      }
    })

    println("=========================")
    println(result.filter(_!=None).collect().toBuffer)

    sc.stop()
  }


}
