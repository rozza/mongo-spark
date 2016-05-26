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

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import org.bson.Document
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql.{Character, _}

class NoSparkConfSpec extends RequiresMongoDB {

  "MongoRDD" should "be able to accept just Read / Write Configs" in {
    val writeConfig = WriteConfig(Map("uri" -> mongoClientURI, "database" -> databaseName, "collection" -> collectionName))
    val readConfig = ReadConfig(Map("uri" -> mongoClientURI, "database" -> databaseName, "collection" -> collectionName))

    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    documents.saveToMongoDB(writeConfig = writeConfig)

    val rdd = MongoSpark.builder().sparkContext(sc).readConfig(readConfig).build().toRDD()
    rdd.count() should equal(10) //scalastyle:ignore
    rdd.map(_.getInteger("test")).collect().toList should equal((1 to 10).toList)
  }

  "DataFrame Readers and Writers" should "be able to accept just options" in {
    val characters = Seq(Character("Gandalf", 1000), Character("Bilbo Baggins", 50)) //scalastyle:ignore

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sc.parallelize(characters).toDF().write
      .option("mode", "Overwrite")
      .option("uri", mongoClientURI)
      .option("database", databaseName)
      .option("collection", collectionName)
      .mongo()

    val ds = sqlContext.read
      .option("uri", mongoClientURI)
      .option("database", databaseName)
      .option("collection", collectionName)
      .mongo().as[Character]

    ds.collect().toList should equal(characters)
  }

  val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("MongoSparkConnector"))

  override def beforeEach(): Unit = {
  }

  override def afterAll() {
    super.afterAll()
    sc.stop()
  }

}
