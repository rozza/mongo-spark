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
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, DataFrame, Dataset, SQLContext}

import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson
import org.bson.{BsonDocument, Document}
import com.mongodb.spark.conf.ReadConfig
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.rdd.partitioner.MongoPartitioner
import com.mongodb.spark.sql.MongoInferSchema
import com.mongodb.spark.sql.MongoRelationHelper.documentToRow
import com.mongodb.spark.{MongoConnector, NotNothing}

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
  def apply[D: ClassTag](sc: SparkContext, connector: MongoConnector): MongoRDD[D] = apply(sc, connector, ReadConfig(sc.getConf))

  /**
   * Creates a MongoRDD
   *
   * @param sc the Spark context
   * @param readConfig the [[com.mongodb.spark.conf.ReadConfig]]
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def apply[D: ClassTag](sc: SparkContext, readConfig: ReadConfig): MongoRDD[D] = apply(sc, MongoConnector(sc.getConf), readConfig)

  /**
   * Creates a MongoRDD
   *
   * @param sc the Spark context
   * @param connector the [[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.conf.ReadConfig]]
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def apply[D: ClassTag](sc: SparkContext, connector: MongoConnector, readConfig: ReadConfig): MongoRDD[D] =
    MongoSplitKeyRDD(sc, connector, readConfig)

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

  private[spark] val connector: Broadcast[MongoConnector]

  private[spark] val readConfig: ReadConfig

  protected def mongoPartitioner: MongoPartitioner

  protected def pipeline: Seq[Bson]

  override def toJavaRDD(): JavaMongoRDD[D] = JavaMongoRDD(this)

  /**
   * Creates a `DataFrame` based on the schema derived from the optional type.
   *
   * '''Note:''' Prefer [[toDS[T<:Product]()*]] as computations will be more efficient.
   *
   * @tparam T The optional type of the data from MongoDB, if not provided the schema will be inferred from the collection
   * @return a DataFrame
   */
  def toDF[T <: Product: TypeTag](): DataFrame = {
    val schema: StructType = MongoInferSchema.reflectSchema[T]() match {
      case Some(reflectedSchema) => reflectedSchema
      case None                  => MongoInferSchema(MongoRDD[BsonDocument](sc, connector.value, readConfig).withPipeline(pipeline))
    }
    toDF(schema)
  }

  /**
   * Creates a `DataFrame` based on the schema derived from the bean class.
   *
   * '''Note:''' Prefer [[toDS[T](beanClass:Class[T])*]] as computations will be more efficient.
   *
   * @param beanClass encapsulating the data from MongoDB
   * @tparam T The bean class type to shape the data from MongoDB into
   * @return a DataFrame
   */
  def toDF[T](beanClass: Class[T]): DataFrame = toDF(MongoInferSchema.reflectSchema[T](beanClass))

  /**
   * Creates a `Dataset` from the collection strongly typed to the provided case class.
   *
   * @tparam T The type of the data from MongoDB
   * @return
   */
  def toDS[T <: Product: TypeTag: NotNothing](): Dataset[T] = {
    val dataFrame: DataFrame = toDF[T]()
    import dataFrame.sqlContext.implicits._
    dataFrame.as[T]
  }

  /**
   * Creates a `Dataset` from the RDD strongly typed to the provided java bean.
   *
   * @tparam T The type of the data from MongoDB
   * @return
   */
  def toDS[T](beanClass: Class[T]): Dataset[T] = toDF[T](beanClass).as(Encoders.bean(beanClass))

  /**
   * Returns a copy with the specified aggregation pipeline
   *
   * @param pipeline the aggregation pipeline to use
   * @return the updated MongoRDD
   */
  def withPipeline[B <: Bson](pipeline: Seq[B]): Self = {
    val codecRegistry: CodecRegistry = connector.value.getMongoClient().getMongoClientOptions.getCodecRegistry
    copy(pipeline = pipeline.map(x => x.toBsonDocument(classOf[Document], codecRegistry))) // Convert to serializable BsonDocuments
  }

  /**
   * Allows to copying of this RDD with changing some of the properties
   */
  protected def copy(
    connector:  Broadcast[MongoConnector] = connector,
    readConfig: ReadConfig                = readConfig,
    pipeline:   Seq[Bson]                 = pipeline
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

  private def toDF(schema: StructType): DataFrame = {
    val rowRDD = MongoRDD[Document](sc, connector.value, readConfig).withPipeline(pipeline).map(doc => documentToRow(doc, schema, Array()))
    new SQLContext(sc).createDataFrame(rowRDD, schema)
  }

}

