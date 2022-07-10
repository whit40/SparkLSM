/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd
import org.apache.spark.SparkContext

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/home/whit/spark-3.2.1-bin-hadoop3.2-scala2.13/README.md" // Should be some file on your system
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
    
    // println("partitions: " + partdata.collect().toList)
    //println(partdata.zipWithIndex())
    partdata.glom().collect().foreach(a => {a.foreach(println);println("=====")})
    
    spark.stop()
  }
}
