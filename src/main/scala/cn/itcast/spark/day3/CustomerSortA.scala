package cn.itcast.spark.day3

import org.apache.spark.{SparkConf, SparkContext}

object ContextOrderA {
//  implicit object Customer2Ordering extends Ordering[GirlA]  {
//    override def compare(x: GirlA, y: GirlA): Int = {
//      if (x.faceValue > y.faceValue) {
//        1
//      } else if (x.faceValue == y.faceValue) {
//        if (x.age > y.age) -1 else 1
//      } else {
//        -1
//      }
//    }
//  }

    implicit val cus2Ordering = new Ordering[GirlA] {
      override def compare(x: GirlA, y: GirlA): Int = {
        if (x.faceValue > y.faceValue) {
          1
        } else if (x.faceValue == y.faceValue) {
          if (x.age > y.age) -1 else 1
        } else {
          -1
        }
      }
    }
}

object CustomerSortA {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomerSortA").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("kk", 90, 25, 1), ("jj", 90, 22, 2), ("aa", 95, 28, 1)))

    import ContextOrderA._
    val rdd2 = rdd1.sortBy(x => GirlA(x._2, x._3), false)
    println(rdd2.collect().toBuffer)

    sc.stop()
  }
}

//case class GirlA(val faceValue: Int, val age: Int) extends Ordered[GirlA] with Serializable {
//  override def compare(that: GirlA): Int = {
//    if (this.faceValue == that.faceValue) {
//      that.age - this.age
//    } else {
//      this.faceValue - that.faceValue
//    }
//  }
//}

case class GirlA(faceValue: Int, age: Int) extends  Serializable
