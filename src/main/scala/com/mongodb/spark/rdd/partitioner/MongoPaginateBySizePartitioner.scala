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

import scala.util.{Failure, Success, Try}

import org.bson.{BsonDocument, BsonValue}
import com.mongodb.MongoCommandException
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Projections, Sorts}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig

/**
 * The pagination by size partitioner.
 *
 * Paginates the collection into partitions based on their size.  Uses the `collStats` command and the average document size to
 * estimate the partition boundaries.
 *
 * $configurationProperties
 *
 *  - [[partitionKeyProperty partitionkey]], the field to partition the collection by. The field should be indexed and contain unique values.
 *     Defaults to `_id`.
 *  - [[partitionSizeMBProperty partitionsizemb]], the size (in MB) for each partition. Defaults to `64`.
 *
 *
 * '''Note:''' This can be a expensive operation as it creates 1 cursor for every estimated `partitionSizeMB`s worth of documents.
 *
 * @since 1.0
 */
class MongoPaginateBySizePartitioner extends MongoPartitioner with MongoPaginationPartitioner {

  private val DefaultPartitionKey = "_id"
  private val DefaultPartitionSizeMB = "64"

  /**
   * The partition key property
   */
  val partitionKeyProperty = "partitionKey".toLowerCase()

  /**
   * The partition size MB property
   */
  val partitionSizeMBProperty = "partitionSizeMB".toLowerCase()

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
        val count = results.getNumber("count").longValue()
        val avgObjSizeInBytes = results.getNumber("avgObjSize").longValue()
        val size = results.getNumber("size").longValue()
        val numberOfPartitions = math.floor(size / partitionSizeInBytes.toFloat).toInt
        val estNumDocumentsPerPartition: Int = math.floor(partitionSizeInBytes.toFloat / avgObjSizeInBytes).toInt

        val rightHandBoundaries = estNumDocumentsPerPartition >= count match {
          case true => Seq.empty[BsonValue]
          case false =>
            val skipValues = (0 to numberOfPartitions.toInt).map(i => i * estNumDocumentsPerPartition)
            calculateSkipPartitions(connector, readConfig, partitionKey, count, skipValues)
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

case object MongoPaginateBySizePartitioner extends MongoPaginateBySizePartitioner
