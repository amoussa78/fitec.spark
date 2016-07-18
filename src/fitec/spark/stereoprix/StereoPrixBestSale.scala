package fitec.spark.stereoprix

import scala.math.random

import org.apache.spark._

object StereoPrixBestSale {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[2]")
    val spark = new SparkContext(conf)
    val inputFile = spark.textFile(args(0))
    val count = inputFile.map(line => (line.split(" ")(5), line.split(" ")(4) ) ).map((_, 1)).reduceByKey(_+_).map{case ((k, v),c) => (k, (v,c)) };
    println(count.toString())
    spark.stop()
  }

}