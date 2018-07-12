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

package com.mongodb.spark.config

import java.util

import com.mongodb.client.model.{Collation, CollationAlternate, CollationCaseFirst, CollationMaxVariable, CollationStrength}
import com.mongodb.spark.notNull
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.bson.{BsonBoolean, BsonDocument, BsonNumber, BsonString, BsonValue}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
 * The `CollationConfig` companion object
 *
 * @since 2.3
 */
object CollationConfig extends MongoInputConfig {

  type Self = CollationConfig

  /**
   * Creates a `CollationConfig` from a `Collation` instance
   *
   * @param collation the collation
   * @return the configuration
   */
  def apply(collation: Collation): CollationConfig = {
    new CollationConfig(Option(collation.getAlternate).map(_.getValue), Option(collation.getBackwards).map(_.booleanValue()),
      Option(collation.getCaseFirst).map(_.getValue), Option(collation.getCaseLevel).map(_.booleanValue()), Option(collation.getLocale),
      Option(collation.getMaxVariable).map(_.getValue), Option(collation.getNormalization).map(_.booleanValue()),
      Option(collation.getNumericOrdering).map(_.booleanValue()), Option(collation.getStrength).map(_.getIntRepresentation))
  }

  /**
   * Creates a CollationConfig from the collation String Document
   *
   * @param collationJson the `Collation.asDocument().toJson()` string value
   * @return the configutaion
   */
  def apply(collationJson: String): CollationConfig = {
    val collationConfig = Try({
      val collationDocument = BsonDocument.parse(collationJson)
      if (collationDocument.isEmpty) {
        CollationConfig()
      } else {
        val options: mutable.Map[String, String] = mutable.Map()
        collationDocument.entrySet().asScala.map({ entry: util.Map.Entry[String, BsonValue] =>
          {
            val key = s"$collationProperty.${entry.getKey}"
            val value = entry.getValue match {
              case s: BsonString  => s.getValue
              case b: BsonBoolean => b.getValue.toString
              case i: BsonNumber  => i.intValue().toString
              case v: BsonValue   => v.toString
            }
            options += key -> value
          }
        })
        apply(options)
      }
    })

    require(collationConfig.isSuccess, s"Invalid Collation document: $collationJson:%n${collationConfig.failed.get}")
    collationConfig.get
  }

  override def apply(options: collection.Map[String, String], default: Option[CollationConfig]): CollationConfig = {
    val collationConfig = Try({
      stripPrefix(options).get(collationProperty)
        .map(json => apply(apply(json).asOptions, default)).getOrElse(default.getOrElse(CollationConfig()))
    })

    require(collationConfig.isSuccess, s"Invalid Collation map $options:%n${collationConfig.failed.get}")
    collationConfig.get
  }

  /**
   * Default configuration
   *
   * @return the configuration
   */
  def create(): CollationConfig = CollationConfig()

  /**
   * Creates a `CollationConfig` from a `Collation` instance
   *
   * @param collation the collation
   * @return the configuration
   */
  def create(collation: Collation): CollationConfig = {
    notNull("collation", collation)
    apply(collation)
  }

  override def create(sparkConf: SparkConf): CollationConfig = {
    notNull("sparkConf", sparkConf)
    apply(sparkConf)
  }

  override def create(options: util.Map[String, String]): CollationConfig = {
    notNull("options", options)
    apply(options.asScala)
  }

  override def create(options: util.Map[String, String], default: CollationConfig): CollationConfig = {
    notNull("options", options)
    notNull("default", default)
    apply(options.asScala, Some(default))
  }

  override def create(javaSparkContext: JavaSparkContext): CollationConfig = {
    notNull("javaSparkContext", javaSparkContext)
    apply(javaSparkContext.getConf)
  }

  override def create(sparkConf: SparkConf, options: util.Map[String, String]): CollationConfig = {
    notNull("sparkConf", sparkConf)
    notNull("options", options)
    apply(sparkConf, options.asScala)
  }

  @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
  override def create(sqlContext: SQLContext): CollationConfig = {
    notNull("sqlContext", sqlContext)
    create(sqlContext.sparkSession)
  }

  override def create(sparkSession: SparkSession): CollationConfig = {
    notNull("sparkSession", sparkSession)
    apply(sparkSession)
  }
}

/**
 * The Collation Config
 *
 * Represents a Collation
 *
 * @since 2.3
 */
case class CollationConfig(private val alternate: Option[String] = None, private val backwards: Option[Boolean] = None,
                           private val caseFirst: Option[String] = None, private val caseLevel: Option[Boolean] = None,
                           private val locale: Option[String] = None, private val maxVariable: Option[String] = None,
                           private val normalization: Option[Boolean] = None, private val numericOrdering: Option[Boolean] = None,
                           private val strength: Option[Int] = None) extends MongoClassConfig {
  require(Try(collation).isSuccess, s"Invalid CollationConfig configuration")

  type Self = CollationConfig

  override def asOptions: collection.Map[String, String] = {
    val options: mutable.Map[String, String] = mutable.Map()
    val collationDoc = collation.asDocument()
    if (!collationDoc.isEmpty) {
      options += CollationConfig.collationProperty -> collationDoc.toJson
    }
    options.toMap
  }

  override def withOption(key: String, value: String): CollationConfig = CollationConfig(this.asOptions + (key -> value))

  override def withOptions(options: collection.Map[String, String]): CollationConfig = CollationConfig(options, Some(this))

  override def withOptions(options: util.Map[String, String]): CollationConfig = withOptions(options.asScala)

  override def asJavaOptions: util.Map[String, String] = asOptions.asJava

  /**
   * The `Collation` that this config represents
   *
   * @return the Collation
   */
  def collation: Collation = {
    val builder = Collation.builder()
    alternate.map(a => builder.collationAlternate(CollationAlternate.fromString(a)))
    backwards.map(b => builder.backwards(b))
    caseFirst.map(c => builder.collationCaseFirst(CollationCaseFirst.fromString(c)))
    caseLevel.map(c => builder.caseLevel(c))
    locale.map(l => builder.locale(l))
    maxVariable.map(m => builder.collationMaxVariable(CollationMaxVariable.fromString(m)))
    normalization.map(m => builder.normalization(m))
    numericOrdering.map(n => builder.numericOrdering(n))
    strength.map(s => builder.collationStrength(CollationStrength.fromInt(s)))
    builder.build()
  }

  /**
   * @return true if any custom collation options have been defined.
   */
  def isDefined: Boolean = {
    alternate.isDefined || backwards.isDefined || caseFirst.isDefined || caseLevel.isDefined || locale.isDefined || maxVariable.isDefined ||
      normalization.isDefined || numericOrdering.isDefined || strength.isDefined
  }

}
