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

package com.mongodb.spark.rdd.partitioner

import java.util

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.bson.{BsonDocument, BsonValue}
import com.mongodb.MongoCommandException
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Aggregates, Projections, Sorts}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig

/**
 * The Sample Partitioner.
 *
 * Uses the average document size and random sampling of the collection to determine suitable partitions for the collection.
 *
 * $configurationProperties
 *
 *  - [[partitionKeyProperty partitionkey]], the field to partition the collection by. The field should be indexed and contain unique values.
 *     Defaults to `_id`.
 *  - [[partitionSizeMBProperty partitionsizemb]], the size (in MB) for each partition. Defaults to `64`.
 *  - [[samplesPerPartitionProperty samplesperpartition]], the number of samples for each partition. Defaults to `10`.
 *
 * *Note:* Requires MongoDB 3.2+
 *
 * @since 1.0
 */
class MongoSamplePartitioner extends MongoPartitioner {
  private val DefaultPartitionKey = "_id"
  private val DefaultPartitionSizeMB = "64"
  private val DefaultSamplesPerPartition = "10"

  /**
   * The partition key property
   */
  val partitionKeyProperty = "partitionKey".toLowerCase()

  /**
   * The partition size MB property
   */
  val partitionSizeMBProperty = "partitionSizeMB".toLowerCase()

  /**
   * The number of samples for each partition
   */
  val samplesPerPartitionProperty = "samplesPerPartition".toLowerCase()

  /**
   * Calculate the Partitions
   *
   * @param connector  the MongoConnector
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @return the partitions
   */
  override def partitions(connector: MongoConnector, readConfig: ReadConfig, pipeline: Array[BsonDocument]): Array[MongoPartition] = {

    Try(PartitionerHelper.collStats(connector, readConfig)) match {
      case Success(results) =>
        val partitionKey = readConfig.partitionerOptions.getOrElse(partitionKeyProperty, DefaultPartitionKey)
        val partitionSizeInBytes = readConfig.partitionerOptions.getOrElse(partitionSizeMBProperty, DefaultPartitionSizeMB).toInt * 1024 * 1024
        val samplesPerPartition = readConfig.partitionerOptions.getOrElse(samplesPerPartitionProperty, DefaultSamplesPerPartition).toInt

        val count = results.getNumber("count").longValue()
        val avgObjSizeInBytes = results.getNumber("avgObjSize").longValue()
        val estNumDocumentsPerPartition: Int = math.floor(partitionSizeInBytes.toFloat / avgObjSizeInBytes).toInt
        val numberOfSamples = math.floor(samplesPerPartition * count / estNumDocumentsPerPartition.toFloat).toInt

        val rightHandBoundaries = estNumDocumentsPerPartition >= count match {
          case true => Seq.empty[BsonValue]
          case false =>
            val samples = connector.withCollectionDo(readConfig, {
              coll: MongoCollection[BsonDocument] =>
                coll.aggregate(List(
                  Aggregates.sample(numberOfSamples),
                  Aggregates.project(Projections.include(partitionKey)),
                  Aggregates.sort(Sorts.ascending(partitionKey))
                ).asJava).into(new util.ArrayList[BsonDocument]()).asScala
            })
            samples.zipWithIndex.collect { case (field, i) if i % samplesPerPartition == 0 => field.get("_id") }
        }

        PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, PartitionerHelper.locations(connector))
      case Failure(ex: MongoCommandException) if ex.getErrorMessage.endsWith("not found.") =>
        logInfo(s"Could not find collection (${readConfig.collectionName}), using a single partition")
        MongoSinglePartitioner.partitions(connector, readConfig, pipeline)
      case Failure(e) =>
        logWarning(s"Could not get collection statistics. Server errmsg: ${e.getMessage}")
        throw e
    }
  }
}

case object MongoSamplePartitioner extends MongoSamplePartitioner
