package fitec.scala.booble

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class BoobleByMinutes {
    def main(args: Array[String]): Unit = {
      
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[2]")
    val spark = new SparkContext(conf)
    val inputFile = spark.textFile(args(0))
    val count = inputFile.map(line => line.split(" ")(3).toDouble)
                                       .reduce(_ + _);
    println(count)
    spark.stop()
  }

}