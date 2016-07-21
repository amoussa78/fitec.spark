package fitec.spark.lastfm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.lib.HashPartitioner

import org.apache.hadoop.mapred.lib.MultipleOutputFormat


import org.apache.spark.SparkContext._

object LastFMListening {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[2]")
    val spark = new SparkContext(conf)
    val inputFile = spark.textFile(args(0)) 

    val input = inputFile.map(line => (line.split(" ")(1), (Integer.parseInt(line.split(" ")(2)), Integer.parseInt(line.split(" ")(3)), Integer.parseInt(line.split(" ")(4)))))
    val result = input.reduceByKey((a,b) => (a._1 + b._1 ,a._2 + b._2, a._3+ b._3)).map(a => (a._1, a._2._1 + a._2._2, a._2._3) ) //filter(isListening(, radio))
    result.saveAsTextFile("/home/user13/recordTest/resultQ2")

    spark.stop()
  }

  def isListening(local: String, radio: String): Boolean = {
    if (Integer.parseInt(local) != 0 || Integer.parseInt(radio) != 0) {
      true
    } else {
      false
    }
  }

}
