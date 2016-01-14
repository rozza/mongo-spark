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

package com.mongodb.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import org.bson.Document
import org.bson.conversions.Bson
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.rdd.partitioner.MongoPartitioner

/**
 * The MongoRDD companion object
 *
 * @since 1.0
 */
object MongoRDD {

  /**
   * Creates a MongoRDD
   *
   * @param sc the Spark context
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def apply[D: ClassTag](sc: SparkContext): MongoRDD[D] = apply(sc, MongoConnector(sc.getConf))

  /**
   * Creates a MongoRDD
   *
   * @param sc the Spark context
   * @param connector the MongoConnector
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def apply[D: ClassTag](sc: SparkContext, connector: MongoConnector): MongoRDD[D] = {
    MongoSplitKeyRDD(sc, connector)
  }

  /**
   * Creates a MongoRDD
   *
   * @param sc the Spark context
   * @param connector the MongoConnector
   * @param splitKey the SplitKey
   * @param maxChunkSize the maximum chunksize
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def apply[D: ClassTag](sc: SparkContext, connector: MongoConnector, splitKey: String, maxChunkSize: Int): MongoRDD[D] = {
    MongoSplitKeyRDD(sc, connector, splitKey, maxChunkSize)
  }

}

/**
 * The abstract MongoRDD class
 *
 * @param sc the Spark context
 * @param dep the RDD's dependencies
 * @tparam D the type of Document to return
 */
abstract class MongoRDD[D: ClassTag](@transient sc: SparkContext, dep: Seq[Dependency[_]]) extends RDD[D](sc, dep) {

  /**
   * This is slightly different than Scala this.type.
   * this.type is the unique singleton type of an object which is not compatible with other
   * instances of the same type, so returning anything other than `this` is not really possible
   * without lying to the compiler by explicit casts.
   * Here SelfType is used to return a copy of the object - a different instance of the same type
   */
  type Self <: MongoRDD[D]

  private[spark] def connector: Broadcast[MongoConnector]

  protected def mongoPartitioner: MongoPartitioner

  protected def pipeline: Seq[Bson]

  override def toJavaRDD(): JavaMongoRDD[D] = JavaMongoRDD(this)

  // TODO - to Dataframe?

  /**
   * Returns a copy with the specified aggregation pipeline
   *
   * @param pipeline the aggregation pipeline to use
   * @return the updated MongoRDD
   */
  def withPipeline[B <: Bson](pipeline: Seq[B]): Self = {
    copy(pipeline = pipeline.map(x => x.toBsonDocument(
      classOf[Document],
      connector.value.getMongoClient().getMongoClientOptions.getCodecRegistry
    ))) // Convert to BsonDocuments as they are serializable
  }

  /**
   * Allows to copying of this RDD with changing some of the properties
   */
  protected def copy(
    connector:        Broadcast[MongoConnector] = connector,
    mongoPartitioner: MongoPartitioner          = mongoPartitioner,
    pipeline:         Seq[Bson]                 = pipeline
  ): Self

  protected def checkSparkContext(): Unit = {
    require(
      Option(sc).isDefined,
      """RDD transformation requires a non-null SparkContext.
      |Unfortunately SparkContext in this MongoRDD is null.
      |This can happen after MongoRDD has been deserialized.
      |SparkContext is not Serializable, therefore it deserializes to null.
      |RDD transformations are not allowed inside lambdas used in other RDD transformations.""".stripMargin
    )
  }

}

