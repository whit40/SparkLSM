/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd
import org.apache.spark.SparkContext

object SimpleApp {

  def partFilter(a:Tuple2(Int, Int)) : Int = {
    
  }
  
  // Insert function here
  
  // Delete function here (?)
  
  // Merge function here
  
  // Update function here
  
  // Function to build index?
  
  // Search function here
  
  
  
  def main(args: Array[String]) {
    val logFile = "/home/whit/spark-3.2.1-bin-hadoop3.2-scala2.13/README.md"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    
    val data = Seq((1,10),(2,10), (3,10),(4,20),(5,10),
    (6,30),(7,50),(8,50),(9,50),(10,30),
    (11,10),(12,10),(13,40),(14,40),(15,40),
    (16,40),(17,50),(18,10),(19,40),(20,40)
    )
    
    val rdddata = spark.sparkContext.parallelize(data)
    val partitioner = new RangePartitioner(6, rdddata)
    
    println("Partitioner has this many partitions: " + partitioner.numPartitions)
    
    println("Get partition with key 1: " + partitioner.getPartition(1))
    
    val partdata = rdddata.partitionBy(partitioner)
    
    partdata.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    
    // map example
    
    val newpartdata = partdata.map(f=> (f._1,f._2 + 1))
    newpartdata.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    
    // Reduce examples
    
    // Min
     println("Min : "+partdata.reduce( (a,b)=> (1 ,a._2 min b._2))._2)
    
    // Max
    println("output max : "+ partdata.reduce( (a,b)=> (1 ,a._2 max b._2))._2)
    
    // Sum
    println("output sum : "+ partdata.reduce( (a,b)=> (1 ,a._2 + b._2))._2)
    
    // Use mappartitions to build level 1 index
    val index = partdata.mapPartitions(x=> (List(x.next._1).iterator)).collect
    index.foreach(println)
    
    // Use level 1 to get partition
    val searchNum :Int = 12
    var partNum :Int = 0;
    for (b <- index) {
    	if (searchNum >= b){
    		partNum = partNum + 1 
    	}
    }
    println("Value " + searchNum + " should be in partition: " + partNum)
    
    // Use partitionpruningRDD to query second level of index
    // val prunedRDD = PartitionPruningRDD(partData, )
    
    spark.stop()
  }
}
