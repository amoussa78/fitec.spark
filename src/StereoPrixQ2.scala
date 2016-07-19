import org.apache.spark._

object StereoPrixQ2 {
  def main (args:Array[String]){
    
    if (args.length<2)   System.err.println("Usage: StereoPrixQ2 Produit le plus vendu par catégorie <in> <out>");
    
    val conf =new SparkConf().setAppName("SPARK: StereoPrix").setMaster("local[2]");
    
    val spark =new SparkContext(conf);
    val lines = spark.textFile(args(0))
    
    val counts1 = lines.map{line=>(line.split(" ")(5),line.split(" ")(4))}.groupByKey()
    val t = counts1.map{ t=>(t._1,{
      
      var  Prod: scala.collection.mutable.Map [String, Int]=scala.collection.mutable.Map();
    	
    	for ( p <- t._2)
    	{
    		if (Prod.contains(p))
    		{
    			var i = Prod.get(p);
    			Prod+=(p-> (i.get + 1));
    		}
    		else
    		{
    			Prod+=(p->1);	
    		}
    	}
      
    var pr="" 
    var max=0
    for ((p,n)<-Prod)
    {
        if(n>max) {pr=p
          max=n}
    }
    	
    
    pr+max.toString()}
    
    
    )
    }

      
      t.saveAsTextFile(args(1))
      
      
    }
}