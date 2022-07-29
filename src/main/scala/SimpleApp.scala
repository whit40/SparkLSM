/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import scala.io.Source

object SimpleApp {
  val spark1 = SparkSession.builder().getOrCreate()
  
  // Parameters
  val maxSize = 2
  val levelCount = 3
  val numPartitions = 2

  var level0 = scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
  //val levelArray = scala.collection.mutable.ArrayBuffer.empty[RDD[Tuple2[Int,Int]]]
  val levelArray = scala.collection.mutable.ArrayBuffer.empty[RDD[Tuple2[Int,Int]]]
  
  for (l <- 0 to levelCount){
    println("blah")
    levelArray += spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
  }
  //var level1 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
  var level2 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
  
  // Insert/Delete function here (~5 lines)
  def modifyLSM(value: Tuple2[Int, Int]) : Boolean = {
    val spark = SparkSession.builder().getOrCreate()
    // Insert value into memtable (level0)
    level0 += value
    
    // Check if we need to merge and merge if needed
    if (level0.length > maxSize){
      // Call merge
      //println("call merge")
      mergePacked(0,1)
    } else {
      //println("no merge needed, size is: " + level0.length)
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
  
  val levelArray2 = scala.collection.mutable.ArrayBuffer.empty[RDD[Array[Tuple2[Int,Int]]]]
  val partArray = scala.collection.mutable.ArrayBuffer.empty[RangePartitioner[Int, Int]]
  val rangeArray = scala.collection.mutable.ArrayBuffer.empty[scala.collection.mutable.ArrayBuffer[Tuple2[Int, Int]]]
  
  for (l <- 0 to levelCount){
    levelArray2 += spark1.sparkContext.emptyRDD[Array[Tuple2[Int, Int]]]
    partArray += new RangePartitioner(numPartitions, spark1.sparkContext.emptyRDD[Tuple2[Int, Int]])
    rangeArray += scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
  }
  
  def mergePacked(levelId1: Int, levelId2: Int) : Unit = {
    println("merging levels "+ levelId1 + " and "+ levelId2)
  
    // type for each level RDD should be: RDD[Array(Tuple2(Int,Int))]
    // Convert memtable to RDD if necessary
    var flatlevel1 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
    if (levelId1 == 0){
      flatlevel1 = spark1.sparkContext.parallelize(level0)
      // reset memtable
      level0 = scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
    
    } else {
      flatlevel1 = levelArray2(levelId1).flatMap(array => array)
    }
    var flatlevel2 = levelArray2(levelId2).flatMap(array => array)
  
    // Flatmap and Merge levels with the parameter Ids
    var flatUnion = flatlevel1 ++ flatlevel2
    levelArray2(levelId1) = spark1.sparkContext.emptyRDD[Array[Tuple2[Int, Int]]]
    partArray(levelId1) = new RangePartitioner(numPartitions, spark1.sparkContext.emptyRDD[Tuple2[Int, Int]])
    println("resetting level ranges for " + levelId1 + " and " + levelId2)
    //rangeArray(levelId1) = scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
    //rangeArray(levelId2) = scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
    rangeArray(levelId1).clear
    rangeArray(levelId2).clear
    
    // use tombstones to remove appropriate vals
    flatUnion = flatUnion.reduceByKey(tomestoneCalc)
    
    flatUnion = flatUnion.filter(a => a._2 != -2)
    val flatSize = flatUnion.count
    partArray(levelId2) = new RangePartitioner(numPartitions, flatUnion)
    flatUnion = flatUnion.partitionBy(partArray(levelId2))
    
    println("Going into packing we have: ")
    flatUnion.glom().collect().foreach(a => {a.foreach(println);println("=====")})
  
    // repack new partition
    
    levelArray2(levelId2) = flatUnion.mapPartitions(iter => {
      var array = iter.toArray
      array = array.sortBy(_._1)
      val firstKey = array(0)._1
      val lastKey = array.last._1
      val boundTuple = (firstKey, lastKey)
      rangeArray(levelId2) += boundTuple
      Iterator({array})
      }
    )
    println("array size before sort is " + rangeArray(levelId2).length)
    rangeArray(levelId2) = rangeArray(levelId2).sortBy(_._1)
    println("sort results in merge +++++++++++++==")
    println("array size is " + rangeArray(levelId2).length)
    rangeArray(levelId2).foreach(println)
    println("end sort results in merge +++++++++++++==")
    
    if (flatSize > (maxSize*levelId2*2)){
      
      if (levelId2 < levelCount){
        mergePacked(levelId2, levelId2+1)
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
  
  // Simple binary search to search final pruned RDD/array
  def binarySearch(array: Array[Tuple2[Int,Int]], target: Int) : Int = {
    var lo = 0
    var hi = array.length - 1
    
    while (lo <= hi){
      var mid = ((hi - lo) / 2) + lo
      
      if (array(mid)._1 == target){
        return array(mid)._2
        
      } else if (array(mid)._1 > target){
        hi = mid - 1 
        
      } else {
        lo = mid + 1
      }
    }
    
    return -1
  }
  
  // Binary search to find the correct partition in a search. Array Parameter is an Array of 2Tuples, where
  // the first value is the first key of the partition, and the second value is the last key of the partition.
  // Returns the partition number where the target would be.
  def rangeBinarySearch(array: scala.collection.mutable.ArrayBuffer[Tuple2[Int,Int]], target: Int) : Int = {
    var lo = 0
    var hi = array.length - 1
    
    array.foreach(println)
    
    while (lo <= hi){
      var mid = ((hi - lo) / 2) + lo
      
      if (array(mid)._1 <= target && array(mid)._2 >= target){
        return mid
        
      } else if (array(mid)._1 > target){
        hi = mid - 1 
        
      } else {
        lo = mid + 1
      }
    }
    
    return -1
  }
  
  // Search function here (~10 lines)
  def search(key: Int) : Seq[Int] = {
   
    val partResults = scala.collection.mutable.ArrayBuffer.empty[Int]
    val level0res = level0.find(_._1 == key)
    if (level0res != None){
      return Seq(level0res.get._2)
    } else {
      partResults += 0
    }
    println("L3 check")
    println(rangeArray(3).length)
    
    for (level <- 1 to levelCount) {
      partResults += partArray(level).getPartition(key)
      println("doing level " + level + "=====================")
      println("result from simple part search: "+ partArray(level).getPartition(key))
      println("result from manual part search: "+ rangeBinarySearch(rangeArray(level), key))
      println("+=+=+=+")
      rangeArray(level).foreach(println)
    }
    
    partResults.foreach(println)
    
    // use partitionpruningrdd to get partition
    for (level <- 1 to levelCount){
      val prunedRDD = new PartitionPruningRDD(levelArray2(level), (part => {part == partResults(level)}))
      
      println("partition results from level " + level  + " have size: " + prunedRDD.count())
      //println(prunedRDD.collect())
      println(prunedRDD.flatMap(list => list).collect.foreach(println))
      
      //val searchResult = prunedRDD.flatMap(list => list).collect.find(_._1 == key)
      val searchResult = binarySearch(prunedRDD.flatMap(list => list).collect, key)
      if (searchResult != -1){
        //println("Find search gives us: " + searchResult.get._2)
        println("binary search gives us " + binarySearch(prunedRDD.flatMap(list => list).collect, key))
        return Seq(searchResult)
      }
      //flatPruned.collect().foreach(println)
    }
    
    // search just that partition for the key
    
    return Seq(-1) 
  } 
  
  
  
  def main(args: Array[String]) {
    val logFile = "/home/whit/spark-3.2.1-bin-hadoop3.2-scala2.13/README.md"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    
    /*
    val exdata1 = Array((2,10),(1,10), (3,10),(4,20))
    val exdata2 = Array((2,10),(1,10), (3,10),(5,20))
    levelArray2(1) = spark.sparkContext.parallelize(Array(exdata1))
    levelArray2(2) = spark.sparkContext.parallelize(Array(exdata2))
    mergePacked(1,2)
    */

    // start with 19 elements
    val data = Seq((2,10),(1,10), (3,10),(4,20),(5,10),
    (6,30),(7,50),(9,50),(10,30),
    (11,10),(12,10),(13,40),(14,40),(15,40),
    (16,40),(17,50),(18,10),(19,40),(20,40)
    )
    
    //level1 = spark.sparkContext.parallelize(data)
    //val partitioner = new RangePartitioner(6, level1)
    
    //println("Partitioner has this many partitions: " + partitioner.numPartitions)

    //level1.glom().collect().foreach(a => {a.foreach(println);println("=====")})

    
    val initPath = "/home/whit/spark-3.1.3-bin-hadoop3.2/simple_ex/src/main/initialstate.txt"
    //println("printing input")
    for (line <- Source.fromFile(initPath).getLines){
      val splitTuple = line.split(",")
      modifyLSM((splitTuple(0).toInt, splitTuple(1).toInt))
      //println(line)
    }
    
    println("Try inserting into rdd")
    /*
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
    */
    println("Level 0 (memtable) is: ")
    println(level0.mkString(" "))
    println("Level 1 is: ")
    
    println(levelArray2(1).glom().collect().foreach(a => {a.foreach(b => b.foreach(println));println("=====")}))
    println("Level 2 is: ")
    println(levelArray2(2).glom().collect().foreach(a => {a.foreach(b => b.foreach(println));println("=====")}))
    println("Level 3 is: ")
    println(levelArray2(3).glom().collect().foreach(a => {a.foreach(println);println("=====")}))
    
    search(8)
    //println("partition for key 24 is: " +  )
    
    //val exRanges = Array((1,7),(8,11),(14,20))
    //println("partition search result is " + rangeBinarySearch(exRanges, 12))
    
    rangeArray(1).foreach(println)
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
