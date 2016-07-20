package fitec.scala.booble

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.lib.HashPartitioner

import org.apache.hadoop.mapred.lib.MultipleOutputFormat

object BoobleByMinutes {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[2]")
    val spark = new SparkContext(conf)
    val inputFile = spark.textFile(args(0))
    // hours | minutes | list<Word> | occurence
    val input = inputFile.map(line => (line.split(" ")(1).split("_")(0), line.split(" ")(1).split("_")(1), line.split(" ")(3).split("\\+"), 1))
    val hoursWords = input.map(x => (find(x._1, x._2), x._3, x._4)).map { case (a, b, c) => (a, (b, c)) }
    val temp = hoursWords.reduceByKey((k, v) => (k._1 ++ v._1, k._2 + v._2)).map(a => (a._1, a._2._1.groupBy(identity).map(z => (z._1, z._2.size)).toList.maxBy(_._2)._1, a._2._2))
    		temp.repartition(9)

    input.saveAsTextFile("/home/user13/recordTest/input")
    temp.saveAsTextFile("/home/user13/recordTest/temp")
    hoursWords.saveAsTextFile("/home/user13/recordTest/hoursWords")


    spark.stop()
  }

  def find(heures: String, minutes: String): String = {
    if (29 < Integer.parseInt(minutes)) {
      "entre " + heures + "h30 et " + heures + "h59"
    } else {
      "entre " + heures + "h00 et " + heures + "h29"
    }
  }
 
}

