package fitec.scala.booble

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BoobleByMinutes {
    def main(args: Array[String]): Unit = {
      
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[2]")
    val spark = new SparkContext(conf)
    val inputFile = spark.textFile(args(0))
    val hoursWords = inputFile.map(line => (line.split(" ")(1).split("_")(0), line.split(" ")(1).split("_")(1), line.split(" ")(1).split("--"), 1))
                                                              .map(x => (find(x._1, x._2), x._3, x._4)).map { case (a,b,c) => (a,(b,c))}
                              
    spark.stop()
  }
    
    def find(heures : String, minutes : String) : String ={
      if (29 < Integer.parseInt(minutes)) {
				"entre " + heures + "h30 et " + heures + "h59"
			} else {
				"entre " + heures + "h00 et " + heures + "h29"
			}
    }

}