/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd._
import org.apache.spark.SparkContext

object SimpleApp {
  val spark1 = SparkSession.builder().getOrCreate()
  val maxSize = 22
  var level1 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
  var level2 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
  //def partFilter(a:Tuple2(Int, Int)) : Int = {
    
  //}
  
  // Insert/Delete function here (~5 lines)
  def modifyLSM(value: Seq[Tuple2[Int, Int]]) : Boolean = {
    val spark = SparkSession.builder().getOrCreate()
    // make insert value an RDD, adjust for ins/del?
    val valueRDD = spark.sparkContext.parallelize(value)
    // Union them and repartition
    level1 = level1 ++ valueRDD
    level1 = level1.sortByKey()
    val partitioner = new RangePartitioner(6, level1)
    level1 = level1.partitionBy(partitioner)
    // Check if we need to merge and merge if needed
    if (level1.count() > maxSize){
      // Call merge
      println("call merge")
      merge()
    } else {
      println("no merge needed, size is: " + level1.count())
    }
    return true
  }
  
  // Merge function here (~10 lines)
  def merge() : Unit = {
    // Merge level1 and level2
    level2 = level1 ++ level2
    level2 = level2.sortByKey()
    level1 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
    
    // use tombstones to remove appropriate vals
    level2 = level2.reduceByKey((v1, v2) => v2)
    
    level2 = level2.filter(a => a._2 != -1)
    level2 = level2.sortByKey()
    val partitioner1 = new RangePartitioner(6, level2)
    level2 = level2.partitionBy(partitioner1)
    
    
    return
  }
  
  
  // Update function here
  
  // Function to build index?
  
  // Search function here (~10 lines)
  
  
  
  def main(args: Array[String]) {
    val logFile = "/home/whit/spark-3.2.1-bin-hadoop3.2-scala2.13/README.md"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    
    /* Simple int data 
    val data = Seq(1,2,3,4,5,
    6,7,8,9,10,
    11,12,13,14,15,
    16,17,18,19,20
    )
    */
    val data = Seq((2,10),(1,10), (3,10),(4,20),(5,10),
    (6,30),(7,50),(9,50),(10,30),
    (11,10),(12,10),(13,40),(14,40),(15,40),
    (16,40),(17,50),(18,10),(19,40),(20,40)
    )
    
    level1 = spark.sparkContext.parallelize(data)
    val partitioner = new RangePartitioner(6, level1)
    
    println("Partitioner has this many partitions: " + partitioner.numPartitions)
    
    println("Get partition with key 1: " + partitioner.getPartition(1))
    
    level1 = level1.partitionBy(partitioner)
    
    level1.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    
    println("Try inserting into rdd")
    var ret = modifyLSM(Seq((21,80)))
    ret = modifyLSM(Seq((18,-1)))
    ret = modifyLSM(Seq((18,20)))
    ret = modifyLSM(Seq((18,-1)))
    ret = modifyLSM(Seq((25,50)))
    println("Level 1 is: ")
    level1.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    println("Level 2 is: ")
    level2.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    
    println("vals for 18 are: " +  level2.lookup(18))
    // map example
    
    //val newpartdata = partdata.map(f=> (f._1,f._2 + 1))
    //newpartdata.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    
    // Reduce examples
    
    // Min
    //println("Min : "+partdata.reduce( (a,b)=> (1 ,a._2 min b._2))._2)
    
    // Max
    //println("output max : "+ partdata.reduce( (a,b)=> (1 ,a._2 max b._2))._2)
    
    // Sum
    //println("output sum : "+ partdata.reduce( (a,b)=> (1 ,a._2 + b._2))._2)
    
    // Use mappartitions to build level 1 index
    //val index = partdata.mapPartitions(x=> (List(x.next._1).iterator)).collect
    //index.foreach(println)
    
    // Use level 1 to get partition
    /*
    val searchNum :Int = 12
    var partNum :Int = 0;
    for (b <- index) {
    	if (searchNum >= b){
    		partNum = partNum + 1 
    	}
    }
    println("Value " + searchNum + " should be in partition: " + partNum)
    */
    
    /*
    val index1 = partdata.mapPartitions(iter => {
      val list = iter.toList
      list.sorted
      new Iterator({list})
      }
    )
    */
    
    // )RDD[Array(Int)]
    
    level2.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    
    spark.stop()
  }
}
