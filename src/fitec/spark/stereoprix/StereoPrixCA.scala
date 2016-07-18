package fitec.spark.stereoprix

import scala.math.random
import org.apache.spark._

object StereoPrixCA {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[2]")
    val spark = new SparkContext(conf)
    val inputFile = spark.textFile(args(0))
    val count = inputFile.map(line => line.split(" ")(2).toDouble)
                                       .reduce(_ + _);
    println(count)
    spark.stop()
  }

}