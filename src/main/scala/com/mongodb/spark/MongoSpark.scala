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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._

import org.bson.Document
import org.bson.conversions.Bson
import com.mongodb.spark.DefaultHelper.DefaultsTo
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.rdd.partitioner.{DefaultMongoPartitioner, MongoPartitioner}

/**
 * The MongoSpark helper allows easy creation of RDDs, DataFrames or Datasets from MongoDB.
 *
 * @since 1.0
 */
object MongoSpark {

  /**
   * Create a builder for configuring the [[MongoSpark]]
   *
   * @return a MongoSession Builder
   */
  def builder(): Builder = new Builder

  /**
   * Builder for configuring and creating a [[MongoSpark]]
   *
   * It requires a `SparkContext` or `SQLContext`
   */
  class Builder {
    private var sqlContext: Option[SQLContext] = None
    private var connector: Option[MongoConnector] = None
    private var partitioner: Option[MongoPartitioner] = None
    private var readConfig: Option[ReadConfig] = None
    private var pipeline: Seq[Bson] = Nil
    private var options: collection.Map[String, String] = Map()

    def build(): MongoSpark = {
      require(sqlContext.isDefined, "The sqlContext must be set")
      val sqlCtxt = sqlContext.get
      val sc = sqlCtxt.sparkContext

      new MongoSpark(
        sqlCtxt,
        partitioner.getOrElse(DefaultMongoPartitioner),
        connector.getOrElse(MongoConnector(ReadConfig(ReadConfig.getOptionsFromConf(sc.getConf) ++ options, readConfig).asOptions)),
        readConfig.getOrElse(ReadConfig(sc.getConf, options)),
        pipeline
      )
    }

    /**
     * Sets the SQLContext from the sparkContext
     *
     * @param sparkContext for the RDD
     */
    def sparkContext(sparkContext: SparkContext): Builder = {
      this.sqlContext = Option(SQLContext.getOrCreate(sparkContext))
      this
    }

    /**
     * Sets the SQLContext from the javaSparkContext
     *
     * @param javaSparkContext for the RDD
     */
    def javaSparkContext(javaSparkContext: JavaSparkContext): Builder = {
      this.sqlContext = Option(SQLContext.getOrCreate(javaSparkContext.sc))
      this
    }

    /**
     * Sets the SQLContext
     *
     * @param sqlContext for the RDD
     */
    def sqlContext(sqlContext: SQLContext): Builder = {
      this.sqlContext = Option(sqlContext)
      this
    }

    /**
     * Append a configuration option
     *
     * These options can be used to configure all aspects of how to connect to MongoDB
     *
     * @param key the configuration key
     * @param value the configuration value
     */
    def option(key: String, value: String): Builder = {
      this.options = this.options + (key -> value)
      this
    }

    /**
     * Set configuration options
     *
     * These options can configure all aspects of how to connect to MongoDB
     *
     * @param options the configuration options
     */
    def options(options: collection.Map[String, String]): Builder = {
      this.options = options
      this
    }

    /**
     * Set configuration options
     *
     * These options can configure all aspects of how to connect to MongoDB
     *
     * @param options the configuration options
     */
    def options(options: java.util.Map[String, String]): Builder = {
      this.options = options.asScala
      this
    }

    /**
     * Sets the [[com.mongodb.spark.MongoConnector]] to use
     *
     * @param connector the MongoConnector
     */
    def connector(connector: MongoConnector): Builder = {
      this.connector = Option(connector)
      this
    }

    /**
     * Sets the [[com.mongodb.spark.rdd.partitioner.MongoPartitioner]] to use
     *
     * @param partitioner the partitioner
     */
    def partitioner(partitioner: MongoPartitioner): Builder = {
      this.partitioner = Option(partitioner)
      this
    }

    /**
     * Sets the [[com.mongodb.spark.config.ReadConfig]] to use
     *
     * @param readConfig the readConfig
     */
    def readConfig(readConfig: ReadConfig): Builder = {
      this.readConfig = Option(readConfig)
      this
    }

    /**
     * Sets the aggregation pipeline to use
     *
     * @param pipeline the aggregation pipeline
     */
    def pipeline(pipeline: Seq[Bson]): Builder = {
      this.pipeline = pipeline
      this
    }
  }

}

/**
 * The MongoSpark class
 *
 * *Note:* Creation of the class should be via [[MongoSpark#builder()]].
 *
 * @since 1.0
 */
case class MongoSpark(sqlContext: SQLContext, partitioner: MongoPartitioner, connector: MongoConnector,
                      readConfig: ReadConfig, pipeline: Seq[Bson]) {

  private def rdd[D: ClassTag]()(implicit e: D DefaultsTo Document): MongoRDD[D] =
    new MongoRDD[D](sqlContext, sqlContext.sparkContext.broadcast(connector), partitioner, readConfig, pipeline)

  /**
   * Creates a `RDD` for the collection
   *
   * @tparam D the datatype for the collection
   * @return a MongoRDD[D]
   */
  def toRDD[D: ClassTag]()(implicit e: D DefaultsTo Document): MongoRDD[D] = rdd[D]

  /**
   * Creates a `JavaRDD` for the collection
   *
   * @return a JavaMongoRDD[Document]
   */
  def toJavaRDD(): JavaMongoRDD[Document] = rdd[Document].toJavaRDD()

  /**
   * Creates a `JavaRDD` for the collection
   *
   * @param clazz   the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return the javaRDD
   */
  def toJavaRDD[D](clazz: Class[D]): JavaMongoRDD[D] = {
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    rdd[D].toJavaRDD()
  }

  /**
   * Creates a `DataFrame` based on the schema derived from the optional type.
   *
   * '''Note:''' Prefer [[toDS[T<:Product]()*]] as computations will be more efficient.
   *  The rdd must contain an `_id` for MongoDB versions < 3.2.
   *
   * @tparam D The optional type of the data from MongoDB, if not provided the schema will be inferred from the collection
   * @return a DataFrame
   */
  def toDF[D <: Product: TypeTag](): DataFrame = rdd[Document].toDF[D]()

  /**
   * Creates a `DataFrame` based on the schema derived from the bean class.
   *
   * '''Note:''' Prefer [[toDS[D](beanClass:Class[D])*]] as computations will be more efficient.
   *
   * @param beanClass encapsulating the data from MongoDB
   * @tparam D The bean class type to shape the data from MongoDB into
   * @return a DataFrame
   */
  def toDF[D](beanClass: Class[D]): DataFrame = rdd[Document].toDF[D](beanClass)

  /**
   * Creates a `Dataset` from the collection strongly typed to the provided case class.
   *
   * @tparam D The type of the data from MongoDB
   * @return
   */
  def toDS[D <: Product: TypeTag: NotNothing](): Dataset[D] = rdd[Document].toDS[D]()

  /**
   * Creates a `Dataset` from the RDD strongly typed to the provided java bean.
   *
   * @tparam D The type of the data from MongoDB
   * @return
   */
  def toDS[D](beanClass: Class[D]): Dataset[D] = rdd[Document].toDS[D](beanClass)

  /**
   * Creates a DataFrameReader with the `MongoDB` underlying output data source.
   *
   * @return the DataFrameReader
   */
  def read(): DataFrameReader = sqlContext.read.options(readConfig.asOptions).format("com.mongodb.spark.sql")

  /**
   * Creates a DataFrameWriter with the `MongoDB` underlying output data source.
   *
   * @param dataFrame the DataFrame to convert into a DataFrameWriter
   * @return the DataFrameWriter
   */
  def write(dataFrame: DataFrame): DataFrameWriter = dataFrame.write.format("com.mongodb.spark.sql")
}

