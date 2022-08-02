/* lsmtree.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import scala.io.Source

object SimpleApp {
  val spark1 = SparkSession.builder().getOrCreate()
  
  // Begin modifiable LSM tree parameters:
  
  // maxSize: the maximum size (number of elements) of the level0 memtable. The size of the subsequent levels scales
  // on a set factor. 
  val maxSize = 2
  
  // levelCount: number of levels in the LSM tree, NOT including level0/memtable. Must be >= 1.
  val levelCount = 3
  
  // numPartitions: number of partitions in the RDD for each level excluding level0/memtable.
  val numPartitions = 2
  
  // initialStatePath: path to the file containing the initial state of the LSM tree. See README for 
  // instructions on the format of the file. 
  val initialStatePath = "/home/whit/spark-3.1.3-bin-hadoop3.2/simple_ex/src/main/initialstate.txt"

  // modFile: path to the file containing the modifications to be made to the LSM tree. 
  val modFile = "/home/whit/spark-3.1.3-bin-hadoop3.2/simple_ex/src/main/initialstate.txt"
  
  // runExamples: enable/disable the running of the examples. 
  val runExamples = true
  
  // End modifiable parameters. 

  var level0 = scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
  val levelArray = scala.collection.mutable.ArrayBuffer.empty[RDD[Array[Tuple2[Int,Int]]]]
  val rangeArray = scala.collection.mutable.ArrayBuffer.empty[scala.collection.mutable.ArrayBuffer[Tuple2[Int, Int]]]
  
  for (l <- 0 to levelCount){
    levelArray += spark1.sparkContext.emptyRDD[Array[Tuple2[Int, Int]]]
    rangeArray += scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
  }
  
  
  /**
   * Inserts or deletes a key-value pair in the LSM tree. A value of -1 represents a deletion. 
   * The modification is inserted into the level0/memtable, and triggers (possibly cascading) merges if needed.
   * Does not return anything. 
   */
  def modifyLSM(value: Tuple2[Int, Int]) : Unit = {
    val spark = SparkSession.builder().getOrCreate()
    level0 += value
    
    // Check if we need to merge and merge if needed
    if (level0.length > maxSize){
      mergePacked(0,1)
    }
  }
  
  
  /**
   * Utility function used in the merge process to determine the correct value for a key. 
   * The reduction by key uses this function to mark keys to cancel, select new value in the case of
   * update or upsert, or do nothing (for example, not removing a tombstone that hasn't reached a deeper level yet
   * where it would take effect)
   * Returns the appropriate value for the key at that point in the reduction. 
   */ 
  def tomestoneCalc(v1: Int, v2:Int) : Int = {
    if (v1 == v2){
      // 1 element with the key in the RDD, don't remove it (not cancelling anything out yet)
      return v1
    }
    
    if (v1 == -1 || v2 == -1) {
      // need to cancel out, return key removal code (-2) to mark for deletion 
      return -2
    } else {
      // otherwise take most recent one
      return v2
    }
  }
  
  
  /**
   * Performs a (possibly) cascading merge on the levels provided as parameters. Flattens each RDD, combines them,
   * takes care of any tombstones or updates/upserts, and repackages the new RDD. May trigger another merge call
   * if the new level is over the size limit. 
   * Does not return anything. 
   */
  def mergePacked(levelId1: Int, levelId2: Int) : Unit = {
  
    // type for each level RDD should be: RDD[Array(Tuple2(Int,Int))]
    // Convert memtable to RDD if necessary
    var flatlevel1 = spark1.sparkContext.emptyRDD[Tuple2[Int, Int]]
    if (levelId1 == 0){
      flatlevel1 = spark1.sparkContext.parallelize(level0)
      level0 = scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
    
    } else {
      flatlevel1 = levelArray(levelId1).flatMap(array => array)
    }
    var flatlevel2 = levelArray(levelId2).flatMap(array => array)
  
    // Flatmap and Merge levels with the parameter Ids
    var flatUnion = flatlevel1 ++ flatlevel2
    levelArray(levelId1) = spark1.sparkContext.emptyRDD[Array[Tuple2[Int, Int]]]
    rangeArray(levelId1).clear
    rangeArray(levelId2).clear
    
    // use tombstones to remove appropriate vals
    flatUnion = flatUnion.reduceByKey(tomestoneCalc)
    
    flatUnion = flatUnion.filter(a => a._2 != -2)
    val flatSize = flatUnion.count
    flatUnion = flatUnion.partitionBy(new RangePartitioner(numPartitions, flatUnion))
  
    // repack new partition
    
    levelArray(levelId2) = flatUnion.mapPartitions(iter => {
      var array = iter.toArray
      array = array.sortBy(_._1)
      Iterator({array})
      }
    )
    
    rangeArray(levelId2) = levelArray(levelId2).mapPartitions(iter => {
      var array = iter.toArray
      val firstKey = array(0)(0)._1
      val lastKey = array(0).last._1
      Iterator((firstKey,lastKey))
      }
    ).collect().to[scala.collection.mutable.ArrayBuffer]
    
    if (flatSize > (maxSize*levelId2*2)){
      
      if (levelId2 < levelCount){
        mergePacked(levelId2, levelId2+1)
      }
    }
  }
  
  
  /**
   * Simple function that performs an update by inserting a deletion and subsequent insert into the tree.
   * Does not return anything. 
   */
  def update(value: Tuple2[Int, Int]) : Unit = {
    modifyLSM((value._1, -1))
    modifyLSM(value)
  }
  
  /**
   * Wrapper function for performing inserts into the LSM tree
   * Does not return anything.
   */
  def insert(key: Int, value: Int) : Unit = {
    modifyLSM((key,value))
  }
  
  /**
   * Wrapper function for performing deletes on the LSM tree
   * Does not return anything.
   */
  def delete(key: Int) : Unit = {
    modifyLSM((key, -1))
  }
  
  /**
   * Simple iterative binary search to search final pruned RDD/array for the key
   * Returns the result of the binary search on the array.
   */
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
  
  
  /**
   * Binary search to find the correct partition of a level for a search. Array Parameter is an Array of 2Tuples, where
   * the first value is the first key of the partition, and the second value is the last key of the 
   * partition (partition boundaries).
   * Returns the partition number where the target would be, with -1 representing the target is not 
   * in any partition range on the level.
   */
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
  
  
  /**
   * Searches all levels of the LSM tree for the parameter key. Searches starting from level0, then level1, and so on.
   * Returns the result of the search, or -1 representing the key isn't found/is marked as deleted.
   */
  def search(key: Int) : Int = {
   
    val partResults = scala.collection.mutable.ArrayBuffer.empty[Int]
    //val level0res = level0.find(_._1 == key)
    val level0idx = level0.lastIndexWhere(_._1 == key)
    if (level0idx != -1){
      val level0res = level0(level0idx)
      return level0res._2
    } else {
      partResults += 0
    }
    
    for (level <- 1 to levelCount) {
      partResults += rangeBinarySearch(rangeArray(level), key)
    }
    
    // use partitionpruningrdd to get partition
    for (level <- 1 to levelCount){
      
      if (partResults(level) != -1){
        val prunedRDD = new PartitionPruningRDD(levelArray(level), (part => {part == partResults(level)}))
      
        val searchResult = binarySearch(prunedRDD.flatMap(list => list).collect, key)
        if (searchResult != -1){
          return searchResult
        }
      
      } 
    }
    return -1 
  } 
  
  /**
   * Prints all levels of the LSM tree in order. Partitions in each level are seperated by "======"
   * Does not return anything.
   */
  def printAllLevels() : Unit = {
    println("Level 0 (memtable) is: ")
    println(level0.mkString(" "))
    
    for (level <- 1 to levelCount) {
      println("Level " + level + " is: ")
      println(levelArray(level).glom().collect().foreach(a => {a.foreach(b => b.foreach(println));println("=====")}))
    }
  }
  
  
  /**
   * Loads an initial state from file. 
   * Does not return anything.
   */
  def loadInitialState() : Unit = {
    for (line <- Source.fromFile(initialStatePath).getLines){
      val splitTuple = line.split(",")
      modifyLSM((splitTuple(0).toInt, splitTuple(1).toInt))
    }
  }
  
  /**
   * Runs a set of modifications from file. 
   * Does not return anything.
   */
  def runModsFromFile() : Unit = {
    for (line <- Source.fromFile(modFile).getLines){
      val splitTuple = line.split(",")
      modifyLSM((splitTuple(0).toInt, splitTuple(1).toInt))
    }
  }
  
  
  /**
   * Resets the LSM tree to a clean state. 
   * Does not return anything.
   */
  def reset() : Unit = {
    level0 = scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
  
    for (level <- 1 to levelCount) {
      levelArray(level) = spark1.sparkContext.emptyRDD[Array[Tuple2[Int, Int]]]
      rangeArray(level) = scala.collection.mutable.ArrayBuffer.empty[Tuple2[Int,Int]]
      
    }
  }
  
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    loadInitialState()

    printAllLevels()
    
    println("Search result for 8 is: "+ search(8))
    
    println("Search result for 60 is: "+ search(60))

    reset()
   
    if (!runExamples){
      spark.stop()
      return
    }
   
    // Example 1
    println("Start Example 1 =================================")
    
    // Inserts, deletes, etc here. In this example we mostly just use modifyLSM directly. 
    
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
    modifyLSM((2,-1))
    modifyLSM((8,30))
    
    printAllLevels()
    
    reset()
    

    // Example 3
    println("Start Example 3 =================================")
    
    // Inserts, deletes, etc here. This is the same example, using the shortcut insert and delete functions.
    
    insert(8,20)
    insert(1,10)
    insert(2,30)
    insert(4,40)
    insert(25,50)
    insert(24,70)
    insert(30,90)
    delete(1)
    delete(24)
    delete(8)
    delete(4)
    insert(20,20)
    insert(10,30)
    update(10, 20)
    delete(2)
    insert(8,30)

    
    printAllLevels()
    
    reset()
    
    // Example 4
    println("Start Example 4 =================================")

    
    // This space is for the user to define their own example(s)

    reset()
    
    spark.stop()
  }
}
