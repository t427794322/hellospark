package cn.itcast.spark.day3

import org.apache.spark.{SparkConf, SparkContext}

object GirlContext {
//  implicit object girl2Ordering extends Ordering[Girl] {
//    override def compare(x: Girl, y: Girl): Int = {
//      if (x.faceValue > y.faceValue) {
//        1
//      } else if (x.faceValue == y.faceValue) {
//        if (x.age > y.age) -1 else 1
//      } else {
//        -1
//      }
//    }
//  }

  implicit val girl2Ordering = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
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

object CustomerSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomerSort").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("yuihatano", 90, 28, 1), ("angelababy", 90, 27, 2),("JuJingYi", 95, 22, 3)))
    import GirlContext._
    val sortRdd = rdd1.sortBy(x => Girl(x._2, x._3), false)
    println(sortRdd.collect().toBuffer)

    sc.stop()
  }
}

//case class Girl(val faceValue: Int, val age: Int) extends Ordered[Girl] with Serializable {
//  override def compare(that: Girl): Int = {
//    if (this.faceValue == that.faceValue) {
//      that.age - this.age
//    } else {
//      this.faceValue - that.faceValue
//    }
//  }
//}

case class Girl(faceValue: Int, age: Int) extends Serializable