package fitec.spark.lastfm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.lib.HashPartitioner

import org.apache.hadoop.mapred.lib.MultipleOutputFormat

object BoobleByMonth {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[2]")
    val spark = new SparkContext(conf)
    val inputFile = spark.textFile(args(0))
    
    val input = inputFile.map(line => (line.split(" ")(0).split("_")(1), line.split(" ")(3).split("\\+"), 1))
    val hoursWords = input.map(x => (find(x._1.toInt), x._2, x._3)).map { case (a, b, c) => (a, (b, c)) }
    val temp = hoursWords.reduceByKey((k, v) => (k._1 ++ v._1, k._2 + v._2)).map(a => (a._1, a._2._1.groupBy(identity).map(z => (z._1, z._2.size)).toList.maxBy(_._2)._1, a._2._2))
    temp.repartition(9)

    input.saveAsTextFile("/home/user13/recordTest/input")
    temp.saveAsTextFile("/home/user13/recordTest/temp")
    hoursWords.saveAsTextFile("/home/user13/recordTest/hoursWords")

    spark.stop()
  }

  def find(month : Int): String = {
    month match {
      case 1 => "janvier"
      case 2 => "fevrier"
      case 3 => "mars"
      case 4 => "avril"
      case 5 => "mai"
      case 6 => "juin"
      case 7 => "juillet"
      case 8 => "auout"
      case 9 => "septembre"
      case 10 => "octobre"
      case 11 => "novembre"
      case 12 => "decembre"
    }
  }

}
