# Mongo Spark Connector Changelog

## 2.3.0
  * [[SPARK-156](https://jira.mongodb.org/browse/SPARK-156)] Updated Spark dependency to 2.3.0. Dropped Scala 2.10 support.
  * [[SPARK-174](https://jira.mongodb.org/browse/SPARK-174)] Updated Mongo Java Driver to 3.8.0
  * [[SPARK-133](https://jira.mongodb.org/browse/SPARK-133)] Added support for MapType when inferring the schema
  * [[SPARK-186](https://jira.mongodb.org/browse/SPARK-186)] Added configuration to disable auto pipeline manipulation with spark sql
  * [[SPARK-188](https://jira.mongodb.org/browse/SPARK-188)] Removed minKey/maxKey bounds from partitioners.
    Partitioners that produce empty querybounds no longer modify the pipeline.
  * [[SPARK-164](https://jira.mongodb.org/browse/SPARK-164)] Added ordered property to WriteConfig.
  * [[SPARK-192](https://jira.mongodb.org/browse/SPARK-192)] Added WriteConfig.forceInsert property.
    DataFrame overwrites will automatically set force insert to true.
  * [[SPARK-178](https://jira.mongodb.org/browse/SPARK-178)] Log partitioner errors to provide users clearer feedback.
  * [[SPARK-102](https://jira.mongodb.org/browse/SPARK-102)] Added AggregationConfig to configure reads from Mongo.

## 2.2.3
  * [[SPARK-187](https://jira.mongodb.org/browse/SPARK-187)] Fixed inferring decimal values with larger scales than precisions.

## 2.2.2
  * [[SPARK-150](https://jira.mongodb.org/browse/SPARK-150)] Fixed MongoShardedPartitioner to work with compound shard keys.
  * [[SPARK-147](https://jira.mongodb.org/browse/SPARK-147)] Fixed writing Datasets for compound shard keys, see WriteConfig#shardKey.
  * [[SPARK-157](https://jira.mongodb.org/browse/SPARK-157)] Fix MongoPaginateByCountPartitioner single item with query exception.
  * [[SPARK-158](https://jira.mongodb.org/browse/SPARK-158)] Fix null handling for String columns
  * [[SPARK-173](https://jira.mongodb.org/browse/SPARK-173)] Improved error messages for cursor not found exceptions

## 2.2.1
  * [[SPARK-151](https://jira.mongodb.org/browse/SPARK-151)] Fix MongoSamplePartitioner $match range bug.

## 2.2.0
  * [[SPARK-127](https://jira.mongodb.org/browse/SPARK-127)] Fix Scala 2.10 compiler error for Java bean type inference.
  * [[SPARK-126](https://jira.mongodb.org/browse/SPARK-126)] Support Spark 2.2.0. Updated Spark dependency to 2.2.0

## 2.1.2
  * [[SPARK-187](https://jira.mongodb.org/browse/SPARK-187)] Fixed inferring decimal values with larger scales than precisions.

## 2.1.1
  * [[SPARK-151](https://jira.mongodb.org/browse/SPARK-151)] Fix MongoSamplePartitioner $match range bug.

## 2.1.0
  * [[SPARK-125](https://jira.mongodb.org/browse/SPARK-125)] Updated Spark dependency to 2.1.1
  * [[SPARK-124](https://jira.mongodb.org/browse/SPARK-124)] Made the maximum batch size when performing bulk updates / inserts configurable.
  * [[SPARK-106](https://jira.mongodb.org/browse/SPARK-106)] Added helpers `MongoSpark.load` helpers for Java users using a SparkSesson.
  * [[SPARK-100](https://jira.mongodb.org/browse/SPARK-100)] Added WriteConfig.replaceDocument to configure how Datasets are saved
  * [[SPARK-39](https://jira.mongodb.org/browse/SPARK-39)] Added support for Decimal type
  * [[SPARK-112](https://jira.mongodb.org/browse/SPARK-112)] Fixed custom partition key bug in MongoSamplePartitioner
  * [[SPARK-122](https://jira.mongodb.org/browse/SPARK-122)] Ensure pagination partitioners can use a covered query
  * [[SPARK-101](https://jira.mongodb.org/browse/SPARK-101)] Add support for partial collection partitioning for non sharded partitioners
  * [[SPARK-103](https://jira.mongodb.org/browse/SPARK-103)] Ensure partitioners handle empty collections

## 2.0.0
  * [[SPARK-84](https://jira.mongodb.org/browse/SPARK-84)] Removed `ConflictType` its not compatible with Spark 2.0
  * [[SPARK-81](https://jira.mongodb.org/browse/SPARK-81)] Set allow disk use to true for all aggregations.
  * [[SPARK-78](https://jira.mongodb.org/browse/SPARK-78)] MongoDataFrameWriterFunctions is no longer public
  * [[SPARK-67](https://jira.mongodb.org/browse/SPARK-67)] MongoRelation no longer public and Default Source updated to return the expected type.
  * [[SPARK-77](https://jira.mongodb.org/browse/SPARK-77)] Removed UDF.undefined support as changes to the Dataset implementation means it is removed when queried.
  * [[SPARK-47](https://jira.mongodb.org/browse/SPARK-47)] Updated API to use SparkSession and deprecated public methods using SQLContext.
  * [[SPARK-20](https://jira.mongodb.org/browse/SPARK-20)] Updated Spark Version to 2.0.0

## 1.1.0
  * [[SPARK-66](https://jira.mongodb.org/browse/SPARK-66)] Saving DataFrames that include an _id will now use a replaceOne with upsert.
  * [[SPARK-71](https://jira.mongodb.org/browse/SPARK-71)] Added support for Spark MapTypes in schemas.
  * [[SPARK-76](https://jira.mongodb.org/browse/SPARK-76)] IsNotNull filter improved so that it also checks the field exists
  * [[SPARK-68](https://jira.mongodb.org/browse/SPARK-68)] Moved InsertableRelation to MongoRelation as its a relation trait not a provider trait.
  * [[SPARK-69](https://jira.mongodb.org/browse/SPARK-69)] Added helpers for defining and querying unsupported MongoDB datatypes.

## 1.0.0
  * [[SPARK-65](https://jira.mongodb.org/browse/SPARK-65)] Performance improvement. Don't append the pipeline when using MongoSinglePartitioner.
  * [[SPARK-63](https://jira.mongodb.org/browse/SPARK-63)] MongoInferSchema now operates on a single partition.
  * [[SPARK-62](https://jira.mongodb.org/browse/SPARK-62)] Made BsonValueOrdering fully serializable.

## 0.4
  * [[SPARK-49](https://jira.mongodb.org/browse/SPARK-49)] Marked internal public code with DeveloperApi annotation.
  * [[SPARK-60](https://jira.mongodb.org/browse/SPARK-60)] Added partitioner to ReadConfig and added custom partitioner options.
  * [[SPARK-54](https://jira.mongodb.org/browse/SPARK-54)] Added a sample and pagination based partitioners.
  * [[SPARK-53](https://jira.mongodb.org/browse/SPARK-53)] Updated DefaultMongoPartitioner implementation.
  * [[SPARK-51](https://jira.mongodb.org/browse/SPARK-51)] Documented partition permissions required.
  * [[SPARK-61](https://jira.mongodb.org/browse/SPARK-61)] Ensure that MongoSpark builder applies overridden options correctly.

## 0.3
  * [[SPARK-58](https://jira.mongodb.org/browse/SPARK-58)] Added the ability to explicitly pass schema when creating a DataFrame.
  * [[SPARK-59](https://jira.mongodb.org/browse/SPARK-59)] Fixed being able to directly connect to a single MongoD in a replicaSet.
  * [[SPARK-56](https://jira.mongodb.org/browse/SPARK-56)] Moved MongoSpark into the Scala API as the main gateway for configuring the connector
    Removed the now redundant `com.mongodb.spark.api.java` namespace.
  * Added abstract class `Logging` so that implementations can be extended easily in Java.
  * [[SPARK-55](https://jira.mongodb.org/browse/SPARK-55)] Made Paritioners public.
  * [[SPARK-52](https://jira.mongodb.org/browse/SPARK-52)] MongoConnector is accessible from the new MongoSpark class or directly.
         Added Java specific methods for withMongoClient, withMongoDatabase and withMongoCollection.
  * [[SPARK-50](https://jira.mongodb.org/browse/SPARK-50)] Made MongoPartition public added tests for custom partitioners.
  * [[SPARK-45](https://jira.mongodb.org/browse/SPARK-45)] Ensure that the SQLContext is reused correctly.

## 0.2
  * [[SPARK-43](https://jira.mongodb.org/browse/SPARK-43)] Ensure that Bson Types are preserved when round tripping dataframes
    * Closed the type system to map `BsonValue` to Spark `DataTypes`
    * Created Case Classes and Java Beans representing unsupported Spark `DataTypes`
  * [[SPARK-42](https://jira.mongodb.org/browse/SPARK-42)] Allow Dataframe readers and writers be configurable just from options
  * [[SPARK-41](https://jira.mongodb.org/browse/SPARK-41)] Don't assume ObjectId's when sampling data
  * [[SPARK-40](https://jira.mongodb.org/browse/SPARK-40)] Fixed Schema inference on arrays with nested structs
  * [[SPARK-38](https://jira.mongodb.org/browse/SPARK-38)] Fixed DataFrame NPE issue handling null data
  * [[SPARK-37](https://jira.mongodb.org/browse/SPARK-37)] Fixed conversion to Numeric types after an RDD had been cached
  * [[SPARK-36](https://jira.mongodb.org/browse/SPARK-36)] Fixed race condition closing MongoClients in RDDs
  * [[SPARK-33](https://jira.mongodb.org/browse/SPARK-33)] Fixed schema inference when saving a DataFrame
  * [[SPARK-32](https://jira.mongodb.org/browse/SPARK-32)] Validated the URI in the Spark Configs.
  * Reorganised the connection string arg position in WriteConfig to match ReadConfig

## 0.1 - Initial release
