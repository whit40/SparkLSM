# SparkLSM: A proof of concept for LSM trees in Apache Spark

## Installation
First, install Apache Spark version 3.1.3 (pre-built for Apache Hadoop 3.2 and later). The instructions and download can be found on on [Spark's website](https://spark.apache.org/downloads.html).

You will also need to install sbt. A guide to do is located here on [the sbt site](https://www.scala-sbt.org/1.x/docs/Setup.html). 

Next, clone SparkLSM into a file in your Spark directory. Your path will look like:
```
YOUR_SPARK_HOME/yourFolderName
```
With the folder contents being those of this repo:
```
src/main
README.md
build.sbt
```
From here, run 
```
sbt package
```
to build the application. Once it has completed, you can run the program using 
```
YOUR_SPARK_HOME/bin/spark-submit --class "lsmtree" --master local[4] target/scala-2.12/lsm-tree-_2.12-1.0.jar
```
If any issues arise during the process, Spark has a useful [quick start guide](https://spark.apache.org/docs/latest/quick-start.html) which may be helpful. 

## Usage

There are a number of parameters you may set to change the characteristics of the tree. All of them are located in the beginning of the main lsmtree.scala file:


> maxSize: the maximum size (number of elements) of the level0 memtable. The size of the subsequent levels scales on a set factor.

> levelCount: number of levels in the LSM tree, NOT including level0/memtable. Must be >= 1.

> numPartitions: number of partitions in the RDD for each level excluding level0/memtable.

> initialStatePath: path to the file containing the initial state of the LSM tree. The format of the file must be a comma-seperated pair of integers on each line, as shown in the example input file.

> modFile: path to the file containing the modifications to be made to the LSM tree. Follows the same format as the initialStatePath.

> runExamples: enable/disable the running of the examples.

Lastly, you may specify your own manual examples in the main file as well. Following the format of the examples, example 4 at the end of the main file has been left blank for this purpose.

Once you have set the parameters as desired, rebuild the project using the *sbt package* command as described in the installation section. 
