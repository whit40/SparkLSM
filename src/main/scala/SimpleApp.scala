/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd._
import org.apache.spark.SparkContext

object SimpleApp {
  val spark1 = SparkSession.builder().getOrCreate()
  
  // Parameters
  val maxSize = 2
  val levelCount = 3
  val numPartitions = 2
  
  var level0 = scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
  val levelArray = scala.collection.mutable.ArrayBuffer.empty[RDD[Tuple2[Int,Int]]]
  
  for (l <- 0 to levelCount){
    println("blah")
    levelArray += spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
  
  }
  var level1 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
  var level2 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
  
  //def partFilter(a:Tuple2(Int, Int)) : Int = {
    
  //}
  
  // Insert/Delete function here (~5 lines)
  def modifyLSM(value: Tuple2[Int, Int]) : Boolean = {
    val spark = SparkSession.builder().getOrCreate()
    // Insert value into memtable (level0)
    level0 += value
    
    //val valueRDD = spark.sparkContext.parallelize(value)
    // Union them and repartition
    //level1 = level1 ++ valueRDD
    //level1 = level1.sortByKey()
    //val partitioner = new RangePartitioner(6, level1)
    //level1 = level1.partitionBy(partitioner)
    // Check if we need to merge and merge if needed
    if (level0.length > maxSize){
      // Call merge
      println("call merge")
      merge(0,1)
    } else {
      println("no merge needed, size is: " + level0.length)
    }
    return true
  }
  
  def tomestoneCalc(v1: Int, v2:Int) : Int = {
    if (v1 == v2){
      // 1 element in the RDD, dont remove it (not cancelling anything out)
      return v1
    }
    
    if (v1 == -1 || v2 == -1) {
      // need to cancel out, return key removal code (-2)  
      return -2
    } else {
      // otherwise take most recent one
      return v2
    }
  }
  
  // Merge function here (~10 lines)
  def merge(levelId1: Int, levelId2: Int) : Unit = {
    println("merging levels "+ levelId1 + " and "+ levelId2)
    // Convert memtable to RDD if necessary
    if (levelId1 == 0){
      levelArray(0) = spark1.sparkContext.parallelize(level0)
      // reset memtable
      level0 = scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
    }
    
    // Merge levels with the parameter Ids
    levelArray(levelId2) = levelArray(levelId1) ++ levelArray(levelId2)
    levelArray(levelId2) = levelArray(levelId2).sortByKey()
    levelArray(levelId1) = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
    
    // use tombstones to remove appropriate vals
    //levelArray(levelId2) = levelArray(levelId2).reduceByKey((v1, v2) => v2)
    levelArray(levelId2) = levelArray(levelId2).reduceByKey(tomestoneCalc)
    
    levelArray(levelId2) = levelArray(levelId2).filter(a => a._2 != -2)
    levelArray(levelId2) = levelArray(levelId2).sortByKey()
    val partitioner1 = new RangePartitioner(numPartitions, levelArray(levelId2))
    levelArray(levelId2) = levelArray(levelId2).partitionBy(partitioner1)
    
    if (levelArray(levelId2).count > (maxSize*levelId2*2)){
      
      if (levelId2 < levelCount){
        merge(levelId2, levelId2+1)
      }
    }
    
    return
  }
  
  
  // Update function here
  def update(value: Tuple2[Int, Int]) : Boolean = {
    modifyLSM((value._1, -1))
    modifyLSM(value)
    return true
  }
  
  // Function to build index?
  
  // Search function here (~10 lines)
  def search(key: Int) : Seq[Int] = {
    return level2.lookup(key)
  } 
  
  
  
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
    
    // start with 19 elements
    val data = Seq((2,10),(1,10), (3,10),(4,20),(5,10),
    (6,30),(7,50),(9,50),(10,30),
    (11,10),(12,10),(13,40),(14,40),(15,40),
    (16,40),(17,50),(18,10),(19,40),(20,40)
    )
    
    level1 = spark.sparkContext.parallelize(data)
    val partitioner = new RangePartitioner(6, level1)
    
    println("Partitioner has this many partitions: " + partitioner.numPartitions)
    
    println("Get partition with key 1: " + partitioner.getPartition(1))
    
    //level1 = level1.partitionBy(partitioner)
    
    //level1.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    
    println("Try inserting into rdd")
    modifyLSM((8,20))
    modifyLSM((1,10))
    modifyLSM((2,30))
    modifyLSM((4,40))
    modifyLSM((25,50))
    modifyLSM((24,70))
    modifyLSM((30,90))
    modifyLSM((1,-1))
    modifyLSM((24,-1))
    modifyLSM((8,-1))
    modifyLSM((4,-1))
    modifyLSM((20,20))
    modifyLSM((10,30))
    update((10, 20))
    println("Level 0 (memtable) is: ")
    println(level0.mkString(" "))
    println("Level 1 is: ")
    println(levelArray(1).glom().collect().foreach(a => {a.foreach(println);println("=====")}))
    println("Level 2 is: ")
    println(levelArray(2).glom().collect().foreach(a => {a.foreach(println);println("=====")}))
    println("Level 3 is: ")
    println(levelArray(3).glom().collect().foreach(a => {a.foreach(println);println("=====")}))
    
    //println("vals for 18 are: " +  level2.lookup(18))
    
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
    /*
    level2.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    
    // Clear everything before next example
    level1 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
    level2 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
    
    // Example 2
    println("Start Example 2 =================================")
    level1 = spark.sparkContext.parallelize(data)
    //val partitioner = new RangePartitioner(6, level1)
    level1 = level1.partitionBy(partitioner)
    
    // Inserts and deletes here
    modifyLSM(Seq((9,-1)))
    modifyLSM(Seq((10,-1)))
    modifyLSM(Seq((1,-1)))
    modifyLSM(Seq((11,-1)))
    modifyLSM(Seq((12,-1)))
    
    println("Level 1 is: ")
    level1.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    println("Level 2 is: ")
    level2.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    
    // Clear everything before next example
    level1 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
    level2 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
    
    // Example 3
    println("Start Example 3 =================================")
    level1 = spark.sparkContext.parallelize(data)
    //val partitioner = new RangePartitioner(6, level1)
    level1 = level1.partitionBy(partitioner)
    
    // Inserts and deletes here
    update(Seq((9,100)))
    update(Seq((9,190)))
    update(Seq((12,100)))
    update(Seq((11,100)))
    
    println("Level 1 is: ")
    level1.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    println("Level 2 is: ")
    level2.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    
        // Clear everything before next example
    level1 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
    level2 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
    
    // Example 4
    println("Start Example 4 =================================")
    level1 = spark.sparkContext.parallelize(data)
    //val partitioner = new RangePartitioner(6, level1)
    level1 = level1.partitionBy(partitioner)
    
    // Inserts and deletes here

    
    println("Level 1 is: ")
    level1.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    println("Level 2 is: ")
    level2.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    */
    spark.stop()
  }
}
