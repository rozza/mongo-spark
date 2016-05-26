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

package com.mongodb.spark.api.java

import java.util

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.reflect.ClassTag

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql._

import org.bson.Document
import org.bson.conversions.Bson
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.rdd.DocumentRDDFunctions
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.rdd.partitioner.MongoPartitioner
import com.mongodb.spark.{MongoConnector, notNull}

/**
 * A Java specific helpers
 *
 * @see [[com.mongodb.spark.MongoSpark]] for other helpers for creating RDD's, DataFrames or Datasets
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
   * It requires a `JavaSparkContext` or `SQLContext`
   */
  class Builder() {
    val underlying = com.mongodb.spark.MongoSpark.builder()

    def build(): MongoSpark = MongoSpark(underlying.build())

    /**
     * Sets the SQLContext from the javaSparkContext
     *
     * @param javaSparkContext for the RDD
     */
    def javaSparkContext(javaSparkContext: JavaSparkContext): Builder = {
      underlying.javaSparkContext(javaSparkContext)
      this
    }

    /**
     * Sets the SQLContext
     *
     * @param sqlContext for the RDD
     */
    def sqlContext(sqlContext: SQLContext): Builder = {
      underlying.sqlContext(sqlContext)
      this
    }

    /**
     * Append a configuration option
     *
     * These options can be used to configure all aspects of how to connect to MongoDB
     *
     * @param key   the configuration key
     * @param value the configuration value
     */
    def option(key: String, value: String): Builder = {
      underlying.option(key, value)
      this
    }

    /**
     * Set configuration options
     *
     * These options can configure all aspects of how to connect to MongoDB
     *
     * @param options the configuration options
     */
    def options(options: Map[String, String]): Builder = {
      underlying.options(options)
      this
    }

    /**
     * Set configuration options
     *
     * These options can configure all aspects of how to connect to MongoDB
     *
     * @param options the configuration options
     */
    def options(options: util.Map[String, String]): Builder = {
      underlying.options(options)
      this
    }

    /**
     * Sets the [[com.mongodb.spark.MongoConnector]] to use
     *
     * @param connector the MongoConnector
     */
    def connector(connector: MongoConnector): Builder = {
      underlying.connector(connector)
      this
    }

    /**
     * Sets the [[com.mongodb.spark.rdd.partitioner.MongoPartitioner]] to use
     *
     * @param partitioner the partitioner
     */
    def partitioner(partitioner: MongoPartitioner): Builder = {
      underlying.partitioner(partitioner)
      this
    }

    /**
     * Sets the [[com.mongodb.spark.config.ReadConfig]] to use
     *
     * @param readConfig the readConfig
     */
    def readConfig(readConfig: ReadConfig): Builder = {
      underlying.readConfig(readConfig)
      this
    }

    /**
     * Sets the aggregation pipeline to use
     *
     * @param pipeline the aggregation pipeline
     */
    def pipeline(pipeline: util.List[Bson]): Builder = {
      underlying.pipeline(pipeline.asScala)
      this
    }
  }

  /**
   * Load data from MongoDB
   *
   * @param jsc the Spark context containing the MongoDB connection configuration
   * @return a MongoRDD
   */
  def load(jsc: JavaSparkContext): JavaMongoRDD[Document] = builder().javaSparkContext(jsc).build().toJavaRDD()

  /**
   * Load data from MongoDB
   *
   * @param jsc the Spark context containing the MongoDB connection configuration
   * @param clazz   the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return a MongoRDD
   */
  def load[D](jsc: JavaSparkContext, clazz: Class[D]): JavaMongoRDD[D] = builder().javaSparkContext(jsc).build().toJavaRDD(clazz)

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database and collection information
   *
   * @param javaRDD the RDD data to save to MongoDB
   * @return the javaRDD
   */
  def save(javaRDD: JavaRDD[Document]): Unit = save(javaRDD, classOf[Document])

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database and collection information
   * Requires a codec for the data type
   *
   * @param javaRDD the RDD data to save to MongoDB
   * @param clazz   the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return the javaRDD
   */
  def save[D](javaRDD: JavaRDD[D], clazz: Class[D]): Unit = {
    notNull("javaRDD", javaRDD)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    DocumentRDDFunctions(JavaRDD.toRDD(javaRDD)).saveToMongoDB()
  }

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database information
   *
   * @param javaRDD        the RDD data to save to MongoDB
   * @param writeConfig the [[com.mongodb.spark.config.WriteConfig]]
   * @return the javaRDD
   */
  def save(javaRDD: JavaRDD[Document], writeConfig: WriteConfig): Unit =
    save(javaRDD, writeConfig, classOf[Document])

  /**
   * Save data to MongoDB
   *
   * Uses the `writeConfig` for the database information
   * Requires a codec for the data type
   *
   * @param javaRDD        the RDD data to save to MongoDB
   * @param writeConfig the [[com.mongodb.spark.config.WriteConfig]]
   * @param clazz          the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return the javaRDD
   */
  def save[D](javaRDD: JavaRDD[D], writeConfig: WriteConfig, clazz: Class[D]): Unit = {
    notNull("javaRDD", javaRDD)
    notNull("writeConfig", writeConfig)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    DocumentRDDFunctions(JavaRDD.toRDD(javaRDD)).saveToMongoDB(writeConfig)
  }

  /**
   * Creates a DataFrameReader with the `MongoDB` underlying output data source.
   *
   * Uses the `SparkConf` for the database and collection information
   *
   * @param sqlContext the SQLContext
   * @return the DataFrameReader
   */
  def read(sqlContext: SQLContext): DataFrameReader = builder().sqlContext(sqlContext).build().underlying.read()

  /**
   * Creates a DataFrameWriter with the `MongoDB` underlying output data source.
   *
   * Uses the `SparkConf` for the database and collection information
   *
   * @param dataFrame the DataFrame to convert into a DataFrameWriter
   * @return the DataFrameWriter
   */
  def write(dataFrame: DataFrame): DataFrameWriter = dataFrame.write.format("com.mongodb.spark.sql")

}

/**
 * The MongoSpark class for use with the Java API
 *
 * *Note:* Creation of the class should be via [[MongoSpark#builder()]].
 *
 * @since 1.0
 */
case class MongoSpark(underlying: com.mongodb.spark.MongoSpark) {

  /**
   * Creates a `JavaRDD` for the collection
   *
   * @return a JavaMongoRDD[Document]
   */
  def toJavaRDD(): JavaMongoRDD[Document] = underlying.toJavaRDD()

  /**
   * Creates a `JavaRDD` for the collection
   *
   * @param clazz the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return the javaRDD
   */
  def toJavaRDD[D](clazz: Class[D]): JavaMongoRDD[D] = underlying.toJavaRDD(clazz)

  /**
   * Creates a `DataFrame` inferring the schema by sampling data from MongoDB.
   *
   * '''Note:''' Prefer [[toDS[T](beanClass:Class[T])*]] as any computations will be more efficient.
   *  The rdd must contain an `_id` for MongoDB versions < 3.2.
   *
   * @return a DataFrame
   */
  def toDF(): DataFrame = underlying.toDF()

  /**
   * Creates a `DataFrame` based on the schema derived from the bean class.
   *
   * '''Note:''' Prefer [[toDS[D](beanClass:Class[D])*]] as computations will be more efficient.
   *
   * @param beanClass encapsulating the data from MongoDB
   * @tparam D The bean class type to shape the data from MongoDB into
   * @return a DataFrame
   */
  def toDF[D](beanClass: Class[D]): DataFrame = underlying.toDF(beanClass)

  /**
   * Creates a `Dataset` from the RDD strongly typed to the provided java bean.
   *
   * @tparam D The type of the data from MongoDB
   * @return
   */
  def toDS[D](beanClass: Class[D]): Dataset[D] = underlying.toDS(beanClass)

  override def productElement(n: Int): Any = underlying.productElement(n)

  override def productArity: Int = underlying.productArity

  override def productIterator: Iterator[Any] = underlying.productIterator

  override def productPrefix: String = underlying.productPrefix

  def copy(sqlContext: SQLContext, partitioner: MongoPartitioner, connector: MongoConnector, readConfig: ReadConfig,
           pipeline: util.List[Bson]): MongoSpark =
    new MongoSpark(underlying.copy(sqlContext, partitioner, connector, readConfig, pipeline.asScala))

  override def canEqual(that: Any): Boolean = underlying.canEqual(that)

  override def equals(that: Any): Boolean = underlying.equals(that)

  override def hashCode(): Int = underlying.hashCode()
}
