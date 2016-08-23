# Mongo Spark Connector Spark SQL

The following code snippets can be found in [SparkSQL.scala](../examples/src/test/scala/tour/SparkSQL.scala).

## Prerequisites

Have MongoDB up and running and the Spark 1.6.x downloaded. This tutorial will use the Spark Shell allowing for instant feedback.
See the [introduction](0-introduction.md) for more information.

Insert some sample data into an empty database:

```scala
import org.bson.Document
import com.mongodb.spark._

val docs = """
 |{"name": "Bilbo Baggins", "age": 50}
 |{"name": "Gandalf", "age": 1000}
 |{"name": "Thorin", "age": 195}
 |{"name": "Balin", "age": 178}
 |{"name": "Kíli", "age": 77}
 |{"name": "Dwalin", "age": 169}
 |{"name": "Óin", "age": 167}
 |{"name": "Glóin", "age": 158}
 |{"name": "Fíli", "age": 82}
 |{"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
sc.parallelize(docs.map(Document.parse)).saveToMongoDB()
```

## Spark SQL

The entry point to Spark SQL is the `SQLContext` class or one of its descendants. To create a basic `SQLContext` all you need is a
`SparkContext`.

```scala
import org.apache.spark.sql.SQLContext

val sc: SparkContext // An existing SparkContext.
val sqlContext = SQLContext.getOrCreate(sc)
```

First enable the Mongo Connector specific functions on the `SQLContext`:

```scala
import com.mongodb.spark.sql._
```

### DataFrames and Datasets

The Mongo Spark Connector provides the `com.mongodb.spark.sql.DefaultSource` class that creates DataFrames and Datasets from MongoDB.
However, the easiest way to create a DataFrame is by using the `MongoSpark` helper:

```scala
val df = MongoSpark.load(sqlContext)  // Uses the SparkConf
df.printSchema()
```

Will return:

```
root
 |-- _id: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- name: string (nullable = true)
```

-----
`MongoSpark.load(sqlContext)` is shorthand for configuring and loading via the DataFrameReader. The following examples are alternative
methods for creating DataFrames:

```scala
sqlContext.loadFromMongoDB() // Uses the SparkConf for configuration
sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://example.com/database.collection"))) // Uses the ReadConfig

sqlContext.read.mongo()
sqlContext.read.format("com.mongodb.spark.sql").load()

// Set custom options:
sqlContext.read.mongo(customReadConfig)
sqlContext.read.format("com.mongodb.spark.sql").options.(customReadConfig.asOptions).load()
```

-----

In the following example we can filter and output the characters with ages under 100:

```scala
df.filter(df("age") < 100).show()
```

```
+--------------------+---+-------------+
|                 _id|age|         name|
+--------------------+---+-------------+
|56eaa0d7ba58b9043...| 50|Bilbo Baggins|
|56eaa0d7ba58b9043...| 77|         Kíli|
|56eaa0d7ba58b9043...| 82|         Fíli|
+--------------------+---+-------------+
```

-----
*Note:* Unlike RDD's when using `filters` with DataFrames or SparkSQL the underlying Mongo Connector code will construct an aggregation
pipeline to filter the data in MongoDB before sending it to Spark.

-----

#### Schema inference and explicitly declaring a schema

By default reading from MongoDB in a `SQLContext` infers the schema by sampling documents from the database.
If you know the shape of your documents then you can use a simple case class to define the schema instead, thus preventing the extra queries.

-----
**Note:** When providing a case class for the schema *only the declared fields* will be returned by MongoDB, helping minimize the data sent across the wire.

-----

The following example creates Character case class and then uses it to represent the documents / schema for the collection:

```scala
case class Character(name: String, age: Int)
val explicitDF = MongoSpark.load[Character](sqlContext)()
explicitDF.printSchema()
```

Outputs:
```
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = false)

```

The following example converts the `DataFrame` into a `Dataset`:

```scala
explicitDF.as[Character]
```

#### RDD to DataFrame / Datasets

The `MongoRDD` class provides helpers to create DataFrames and Datasets directly:

```scala
val dataframeInferred = MongoSpark.load[Character](sqlContext)
val dataframeExplicit = MongoSpark.load[Character](sqlContext)
val dataset = MongoSpark.load[Character](sqlContext).as[Character]()
```

### SQL queries

Spark SQL works on top of DataFrames, to be able to use SQL you need to register a temporary table first and then you can run SQL queries
over the data.

The following example registers a "characters" table and then queries it to find all characters that are 100 or older.

```scala
val characters = MongoSpark.load(sqlContext).toDF[Character]()
characters.registerTempTable("characters")

val centenarians = sqlContext.sql("SELECT name, age FROM characters WHERE age >= 100")
centenarians.show()
```

-----
**Note:** You must use the same `SQLContext` that registers the table when querying it.

-----

### Saving DataFrames

The connector provides the ability to persist data into MongoDB.

In the following example we save the centenarians into the "hundredClub" collection:

```scala
MongoSpark.save(centenarians.write.option("collection", "hundredClub"))
println("Reading from the 'hundredClub' collection:")
MongoSpark.load[Character](sqlContext, ReadConfig(Map("collection" -> "hundredClub"), Some(ReadConfig(sqlContext)))).show()
```

Outputs:

```
+-------+----+
|   name| age|
+-------+----+
|Gandalf|1000|
| Thorin| 195|
|  Balin| 178|
| Dwalin| 169|
|    Óin| 167|
|  Glóin| 158|
+-------+----+
```

-----
`MongoSpark.save(dataFrameWriter)` is shorthand for configuring and saving via the DataFrameWriter. The following examples are alternative
methods for writing DataFrames to MongoDB:

```scala
dataFrameWriter.write.mongo()
dataFrameWriter.write.format("com.mongodb.spark.sql").save()
```

-----

## DataTypes

Spark supports a limited number of data types, to ensure that all bson types can be round tripped in and out of Spark DataFrames /
Datasets. Custom StructTypes are created for any unsupported Bson Types. The following table shows the mapping between the Bson Types and
Spark Types:

Bson Type               | Spark Type
------------------------|----------------------------------------------------------------------
`Document`              | `StructType`
`Array`                 | `ArrayType`
`32-bit integer`        | `Integer`
`64-bit integer`        | `Long`
`Binary data`           | `Array[Byte]` or `StructType`: `{ subType: Byte, data: Array[Byte]}`
`Boolean`               | `Boolean`
`Date`                  | `java.sql.Timestamp`
`DBPointer`             | `StructType`: `{ ref: String , oid: String}`
`Double`                | `Double`
`JavaScript`            | `StructType`: `{ code: String }`
`JavaScript with scope` | `StructType`: `{ code: String , scope: String }`
`Max key`               | `StructType`: `{ maxKey: Integer }`
`Min key`               | `StructType`: `{ minKey: Integer }`
`Null`                  | `null`
`ObjectId`              | `StructType`: `{ oid: String }`
`Regular Expression`    | `StructType`: `{ regex: String , options: String }`
`String`                | `String`
`Symbol`                | `StructType`: `{ symbol: String }`
`Timestamp`             | `StructType`: `{ time: Integer , inc: Integer }`
`Undefined`             | `StructType`: `{ undefined: Boolean }`

### Dataset support

To help better support Datasets, the following Scala case classes and JavaBean classes have been created to represent the unsupported Bson
Types:

Bson Type               | Scala case class                       | JavaBean
------------------------|----------------------------------------|----------------------------------------------
                        | `com.mongodb.spark.sql.fieldTypes`     | `com.mongodb.spark.sql.fieldTypes.api.java.`
`Binary data`           | `Binary`                               | `Binary`
`DBPointer`             | `DBPointer`                            | `DBPointer`
`JavaScript`            | `JavaScript`                           | `JavaScript`
`JavaScript with scope` | `JavaScriptWithScope`                  | `JavaScriptWithScope`
`Max key`               | `MaxKey`                               | `MaxKey`
`Min key`               | `MinKey`                               | `MinKey`
`ObjectId`              | `ObjectId`                             | `ObjectId`
`Regular Expression`    | `RegularExpression`                    | `RegularExpression`
`Symbol`                | `Symbol`                               | `Symbol`
`Timestamp`             | `Timestamp`                            | `Timestamp`
`Undefined`             | `Undefined`                            | `Undefined`

For convenience all Bson Types can be represented as a String value as well, however these values lose all their type information and if
saved back to MongoDB they would be stored as a String.

### Defining and filtering unsupported bson data types

As not all Bson types have equivalent Spark types querying them outside of using Datasets can be verbose and requires in depth knowledge of
the `StructType` that can be used to represent those types.

To help users with these types there are `StructField` helpers that can be used when defining the schema for a DataFrame. There are also
custom helpers that can be used as [User Defined Functions](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.expressions.UserDefinedFunction)
to aid the queryability of the data. To enable all helper functions set the `registerSQLHelperFunctions` configuration option to true,
alternatively you can manually register the required methods.

Bson Type               | StructField helpers                  | User Defined Function helpers
------------------------|--------------------------------------|--------------------------------------------------------------
                        | `com.mongodb.spark.sql.StructFields` | `com.mongodb.spark.sql.helpers.UDF`
`Binary data`           | `binary`                             | `binary` / `binaryWithSubType`
`DBPointer`             | `dbPointer`                          | `dbPointer`
`JavaScript`            | `javascript`                         | `javascript`
`JavaScript with scope` | `javascriptWithScope`                | `javascriptWithScope`
`Max key`               | `maxKey`                             | `maxKey`
`Min key`               | `minKey`                             | `minKey`
`ObjectId`              | `objectId`                           | `objectId`
`Regular Expression`    | `regularExpression`                  | `regularExpression` / `regularExpressionWithOptions`
`Symbol`                | `symbol`                             | `symbol`
`Timestamp`             | `timestamp`                          | `timestamp`
`Undefined`             | `undefined`                          | `undefined`

Below is an example of using the helpers when defining and querying an `ObjectId` field:

```scala
// Load sample data
import org.bson.Document
import org.bson.types.ObjectId
import com.mongodb.spark._
import com.mongodb.spark.sql._

val objectId = "123400000000000000000000"
val newDocs = Seq(new Document("_id", new ObjectId(objectId)).append("a", 1), new Document("_id", new ObjectId()).append("a", 2))
MongoSpark.save(sc.parallelize(newDocs))

// Set the schema using the ObjectId StructFields helper
import org.apache.spark.sql.types.DataTypes
import com.mongodb.spark.sql.helpers.StructFields

val schema = DataTypes.createStructType(Array(
  StructFields.objectId("_id", nullable = false),
  DataTypes.createStructField("a", DataTypes.IntegerType, false))
)

// Create a dataframe with the helper functions registered
val df1 = MongoSpark.read(sqlContext).schema(schema).option("registerSQLHelperFunctions", "true").load()

// Query using the ObjectId string
df1.filter(s"_id = ObjectId('$objectId')").show()
```

Outputs:

```
+--------------------+---+
|                 _id|  a|
+--------------------+---+
|[1234000000000000...|  1|
+--------------------+---+
```

[Next - Configuring](2-configuring.md)
