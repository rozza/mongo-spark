# Mongo Spark Connector Configuration

The following table describes the various configuration options for the Spark Connector. 

The following connection options can be set via the `SparkConf` object. These are prefixed with `spark.` so that they can be recognized
from the spark-shell and can be passed via --conf settings or via $SPARK_HOME/conf/spark-default.conf.

------
**Note:** It is expected that users will configure all the settings in the `SparkConf` at the start and then the Mongo Connector will use those settings as the defaults.

Various methods in the Mongo Connector API will also accept an override for a [`ReadConfig`](../src/main/scala/com/mongodb/spark/config/ReadConfig.scala) or a [`WriteConfig`](src/main/scala/com/mongodb/spark/config/WriteConfig.scala). This is so you can override any values set in the `SparkConf`. (Refer to the source for these methods but in general its where data is loaded or saved to MongoDB). 

In the Spark API there are some methods that accept extra options in the form of a `Map[String, String]`.  For example the `DataFrameReader` or `DataFrameWriter` API's.  Custom `ReadConfig` or `WriteConfig` settings can easily be converted into a `Map` via the `asOptions()` method.

------

## Input Configuration

The following options are available:

Property name          | Description                                                       | Default value
-----------------------|-------------------------------------------------------------------|--------------------
uri                    | The connection string                                             |
database               | The database name to read data from                               |
collection             | The collection name to read data from                             |
localThreshold         | The threshold for choosing a server from multiple MongoDB servers | 15 ms
readPreference.name    | The name of the `ReadPreference` to use                           | Primary
readPreference.tagSets | The `ReadPreference` TagSets to use                               |
readConcern.level      | The `ReadConcern` level to use                                    |
sampleSize             | The sample size to use when inferring the schema                  | 1000
partitioner            | The class name of the partitioner to use to partition the data    | MongoDefaultPartitioner

-----
**Note**: When setting input configurations in the `SparkConf` then the prefix `spark.mongodb.input.` is required.

-----

## Output Configuration

The following options are available on `SparkConf` object:

Property name           | Description                                                       | Default value
------------------------|-------------------------------------------------------------------|--------------------
uri                     | The connection string                                             |
database                | The database name to write data to                                |
collection              | The collection name to write data to                              |
localThreshold          | The threshold for choosing a server from multiple MongoDB servers | 15 ms
writeConcern.w          | The write concern w value                                         | (WriteConcern.ACKNOWLEDGED)
writeConcern.journal    | The write concern journal value                                   |
writeConcern.wTimeoutMS | The write concern wTimeout value                                  |

-----
**Note**: When setting output configurations in the `SparkConf` then the prefix `spark.mongodb.input.` is required.

-----

## Configuring via the uri

These configurations are deliberately kept simple all that is required is the uri, the database and collection and name to connect.
Any configurations via the uri will override any default values, so the uri can be used to configure the ReadPreference or the WriteConcern.

An example configuration for the uri is:
```
spark.mongodb.input.uri=mongodb://127.0.0.1/databaseName.collectionName?readPreference=primaryPreferred
```

Which is the same as:
```
spark.mongodb.input.uri=mongodb://127.0.0.1/
spark.mongodb.input.database=databaseName
spark.mongodb.input.collection=collectionName
spark.mongodb.input.readPreference.name=primaryPreferred
```

## Configuring Partitioners

The default Partitioner is the `MongoDefaultPartitioner`, it samples the database. 

Alternative partitioner implementations can be configured via the `partitioner` configuration option. For custom implementations of the 
`MongoPartitioner` trait the full class name must be provided. If no package names are provided then the default 
`com.mongodb.spark.rdd.partitioner` package is used.

The available Partitioners are:

Partitioner name                | Description
--------------------------------|-------------
MongoSamplePartitioner          | A general purpose partitioner for all deployments. <br>Uses the average document size and random sampling of the collection to determine suitable partitions for the collection.<br>Requires MongoDB 3.2
MongoShardedPartitioner         | A partitioner for sharded clusters. <br>Partitions the collection based on the data chunks. <br>Requires read access to the config database.
MongoSplitVectorPartitioner     | A partitioner for single nodes or replicaSets. Uses the `SplitVector` command on the primary node to determine the partitions of the database. <br>Requires `SplitVector` command privilege.
MongoPaginateByCountPartitioner | A general purpose partitioner for all deployments. <br>Creates a specific number of partitions.<br>Slow as requires a query for every partition.
MongoPaginateBySizePartitioner  | A general purpose partitioner for all deployments. <br>Creates partitions based on data size. Slow as requires a query for every partition.


## Configuration via system properties

The MongoConnector includes a cache for MongoClients, so workers can share the MongoClient across threads. As the cache is setup before the
Spark Configuration is available it can only be configured via a System Property:

System Property name         | Description                                                     | Default value
-----------------------------|-----------------------------------------------------------------|--------------------
spark.mongodb.keep_alive_ms  | The length of time to keep a MongoClient available for sharing  | 5000
