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

package com.mongodb.spark.sql

import scala.reflect.ClassTag

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import org.bson.Document
import com.mongodb.spark.DefaultHelper.DefaultsTo
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD

/**
 * Helpers to create [[com.mongodb.spark.rdd.MongoRDD]] in the current `SQLContext`.
 *
 * @param sqlContext the SQLContext
 * @since 1.0
 */
case class SparkSQLContextFunctions(@transient sqlContext: SQLContext) extends Serializable {

  @transient private val sc: SparkContext = sqlContext.sparkContext
  @transient private val sparkConf: SparkConf = sc.getConf

  /**
   * Creates a MongoRDD using the current SQLContext
   *
   * @tparam D the type of Document to return from MongoDB - defaults to Document
   * @return a MongoRDD
   */
  def loadFromMongoDB[D: ClassTag]()(implicit e: D DefaultsTo Document): MongoRDD[D] =
    MongoSpark.builder().sqlContext(sqlContext).build().toRDD[D]()

}
