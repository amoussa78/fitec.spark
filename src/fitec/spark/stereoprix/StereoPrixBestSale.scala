package fitec.spark.stereoprix

import scala.math.random

import org.apache.spark._

object StereoPrixBestSale {
  def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("Spark_Prix").setMaster("local[2]")
					val spark = new SparkContext(conf)
					var categ = spark.textFile(args(0)).map(line => (line.split(" ")(5),line.split(" ")(4)))
					var categlist = categ.map((_, 1L)).reduceByKey(_ + _).map{ case ((k, v), cnt) => (k, (v, cnt)) }.groupByKey().map{c=> (c._1,c._2.maxBy(_._2))}.map( b => (b._1,b._2._1))//.toLocalIterator.toList
					categlist.sortByKey().foreach(println(_))
					categlist.saveAsTextFile("/home/user13/outputStereoPrix")
					spark.stop()
	}

}