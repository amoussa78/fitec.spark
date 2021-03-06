package fitec.spark.lastfm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.lib.HashPartitioner

import org.apache.hadoop.mapred.lib.MultipleOutputFormat

object LastFMListener {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[2]")
    val spark = new SparkContext(conf)
    val inputFile = spark.textFile(args(0)) 

    val input = inputFile.map(line => (line.split(" ")(1), line.split(" ")(2), line.split(" ")(3)))
    val result = input.filter(x => isListening(x._2,x._3)).map(x =>(x._1, 1)).reduceByKey(_+_)
    result.saveAsTextFile("/home/user13/recordTest/result")

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
