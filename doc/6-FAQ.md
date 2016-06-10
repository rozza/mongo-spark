# Mongo Spark Connector FAQ

## How can I achieve data locality?

MongoDB can be configured in multiple ways: Standalone, ReplicaSet, Sharded Standalone, Sharded ReplicaSet.  
In any configuration the Mongo Spark connector sets the preferred location for an RDD to be where the data is:

* For a non sharded system it sets the preferred location to be the hostname(s) of the standalone or replicaSet.
* For a sharded system it sets the preferred location to be the hostname(s) of where the chunk data is.

To achieve full data locality you should ensure:

  * That there is a Spark Worker on one of the hosts or one per shard.
  * That you use a `nearest` ReadPreference to read from the local `mongod`.
  * If sharded you should have a `MongoS` on the same nodes and use `localThreshold` configuration to connect to the nearest `MongoS`.


## How do I interact with Spark Streams?

Spark streams can be considered as a potentially infinite source of RDDs, therefore anything you can do with an RDD you can do with the
results of a Spark Stream. The following example is adapted from the 
[Spark streaming guide](http://spark.apache.org/docs/latest/streaming-programming-guide.html) and  can be found in 
[SparkStreams.scala](../examples/src/test/scala/tour/SparkStreams.scala):

```scala
import com.mongodb.spark.sql._
import org.apache.spark.streaming._

val sc = ...                // existing SparkContext
val ssc = new StreamingContext(sc, Seconds(1))

// Connect to netcat ( nc -lk 9999 )
val lines = ssc.socketTextStream("localhost", 9999)

// Calculate the word counts
val words = lines.flatMap(_.split(" "))
val pairs = words.map(word => (word, 1))
val wordCounts = pairs.reduceByKey(_ + _)

// Save the word counts for each 1 Second time window into MongoDB
wordCounts.foreachRDD({ rdd =>
  val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
  import sqlContext.implicits._

  val wordCounts = rdd.map({ case (word: String, count: Int) => WordCount(word, count) }).toDF()

  // Save to MongoDB
  wordCounts.write.mode("append").mongo()
})

// Start the computation and await for the computation to terminate
ssc.start()
ssc.awaitTermination()

```

Note: The Mongo Spark Connector only supports streams a sink.

## What permissions do the partitioners require?

Partitioner                         | Permissions required
------------------------------------|-----------------------------------------------------------------------------------------
`MongoFixedNumberPartitioner`       | Read collection
`MongoPaginationPartitioner`        | Read collection
`MongoSamplePartitioner`            | Read collection
`MongoSinglePartitioner`            | Read collection
`MongoShardedPartitioner`           | Read `config` database. Reads from the `chunks` and `shards` collections.
`MongoSplitVectorPartitioner`       | Runs the `SplitVector` command. Requires `clusterManager` role or a custom permission.

The `DefaultMongoPartitioner` is a special case as for sharded collections it uses the `MongoShardedPartitioner` otherwise for MongoDB 3.2+ 
it will use the `MongoSamplePartitioner` but will fallback to the `MongoPaginationPartitioner` for older versions.


## Unrecognized pipeline stage name: '$sample' error.

In MongoDB deployments with mixed mongod versions, it is possible to get an `Unrecognized pipeline stage name: '$sample'` error.
To mitigate this please explicitly configure which partitioner to use and explicitly define the Schema when using DataFrames.

-----

[Next - Changelog](7-Changelog.md)
