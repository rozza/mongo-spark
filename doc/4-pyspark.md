# Mongo Spark Connector Python

The following code snippets can be found in [introduction.py](../examples/src/test/python/tour/introduction.py).

## Prerequisites

Have MongoDB up and running and the Spark 1.6.x downloaded. See the [introduction](0-introduction.md) for more information on getting started.

You can run the interactive pyspark shell like so:

```
./bin/pyspark --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.coll?readPreference=primaryPreferred" \
              --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.coll" \
              --packages org.mongodb.spark:mongo-spark-connector_2.10:0.2
```

## The Python API Basics

The python API works via DataFrames and uses underlying Scala DataFrame.

## DataFrames and Datasets

Creating a dataframe is easy you can either load the data via `DefaultSource` ("com.mongodb.spark.sql.DefaultSource").

First, in an empty collection we load the following data:

```python
charactersRdd = sc.parallelize([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
                                ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)])
characters = sqlContext.createDataFrame(charactersRdd, ["name", "age"])
characters.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()
```

Then to load the characters into a DataFrame via the standard source method:

```python
df = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource").load()
df.printSchema()
```

Will return:

```
root
 |-- _id: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- name: string (nullable = true)
```

### SQL

Just like the Scala examples, SQL can be used to filter data. In the following example we register a temp table and then filter and output 
the characters with ages under 100:

```python
df.registerTempTable("characters")
centenarians = sqlContext.sql("SELECT name, age FROM characters WHERE age >= 100")
centenarians.show()
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
|    Oin| 167|
|  Gloin| 158|
+-------+----+
```

-----

[Next - SparkR](5-sparkR.md)
