/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.spark

import org.scalatest.FlatSpec

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import org.bson.{BsonDocument, Document}
import com.mongodb.client.model.{Aggregates, Filters}
import com.mongodb.spark.rdd.MongoRDD

class MongoRDDSpec extends FlatSpec with RequiresMongoDB {
  val counters =
    """
      | {counter: 0}
      | {counter: 1}
      | {counter: 2}
    """.stripMargin.trim.stripMargin.split("[\\r\\n]+").toSeq

  "MongoRDD" should "be easily created from the SparkContext" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()
    val mongoRDD: MongoRDD[Document] = sc.fromMongoDB()

    mongoRDD.count() shouldBe 3
    mongoRDD.map(x => x.getInteger("counter")).collect() should contain theSameElementsInOrderAs Seq(0, 1, 2)
  }

  it should "be able to handle non existent collections" in withSparkContext() { sc =>
    sc.fromMongoDB().count() shouldBe 0
  }

  it should "be able to query via a pipeline" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    sc.fromMongoDB().withPipeline(List(Document.parse("{$match: { counter: {$gt: 0}}}"))).count() shouldBe 2
    sc.fromMongoDB().withPipeline(List(BsonDocument.parse("{$match: { counter: {$gt: 0}}}"))).count() shouldBe 2
    sc.fromMongoDB().withPipeline(List(Aggregates.`match`(Filters.gt("counter", 0)))).count() shouldBe 2
  }

  it should "be able to handle different collection types" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    val mongoRDD: MongoRDD[BsonDocument] = sc.fromMongoDB[BsonDocument]()
    mongoRDD.count() shouldBe 3
  }

  it should "be able to create a DataFrame by inferring the schema" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    val _idField: StructField = createStructField("_id", DataTypes.StringType, true)
    val countField: StructField = createStructField("counter", DataTypes.IntegerType, true)
    val expectedSchema: StructType = createStructType(Array(_idField, countField))

    val dataFrame: DataFrame = sc.fromMongoDB().toDF()
    dataFrame.schema should equal(expectedSchema)
    dataFrame.count() should equal(3)
  }

  it should "be able to create a DataFrame when provided a case class" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    val expectedSchema: StructType = ScalaReflection.schemaFor[Counter].dataType.asInstanceOf[StructType]
    val dataFrame: DataFrame = sc.fromMongoDB().toDF[Counter]()
    dataFrame.schema should equal(expectedSchema)
    dataFrame.count() should equal(3)
  }

  it should "be able to create a Dataset when provided a case class" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    val expectedSchema: StructType = ScalaReflection.schemaFor[Counter].dataType.asInstanceOf[StructType]
    val dataset: Dataset[Counter] = sc.fromMongoDB().toDS[Counter]()
    dataset.schema should equal(expectedSchema)
    dataset.count() should equal(3)
  }

  it should "not allow Nothing when trying to create a Dataset" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    "sc.fromMongoDB().toDS()" shouldNot compile
    "sc.fromMongoDB().toDS[Nothing]()" shouldNot compile
  }
}
