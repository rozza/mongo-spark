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
import com.mongodb.client.model.Projections
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig

/**
 * The Pagination Partitioner.
 *
 * Uses the `collStats` command and the average document size to estimate the partition boundaries.
 *
 * *Note:* This can be a expensive opertation as it creates 1 cursor for every estimated `partitionSizeMB`s worth of documents.
 *
 * @since 1.0
 */
case object MongoPaginationPartitioner extends MongoPartitioner {

  /**
   * Calculate the Partitions
   *
   * @param connector  the MongoConnector
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @return the partitions
   */
  override def partitions(connector: MongoConnector, readConfig: ReadConfig): Array[MongoPartition] = {
    require(readConfig.partitionKey == "_id", "The MongoPaginationPartitioner can only partition on `_id`")

    Try(PartitionerHelper.collStats(connector, readConfig)) match {
      case Success(results) =>
        val partitionKey = "_id"
        val partitionSizeInBytes = readConfig.partitionSizeMB * 1024 * 1024
        val count = results.getNumber("count").longValue()
        val avgObjSizeInBytes = results.getNumber("avgObjSize").longValue()
        val size = results.getNumber("size").longValue()
        val numberOfPartitions = math.ceil(size / partitionSizeInBytes.toFloat).toInt
        val estNumDocumentsPerPartition: Int = math.ceil(partitionSizeInBytes.toFloat / avgObjSizeInBytes).toInt

        val rightHandBoundaries = estNumDocumentsPerPartition >= count match {
          case true => Seq.empty[BsonValue]
          case false =>
            val skipValues = (0 to numberOfPartitions.toInt).map(i => i * estNumDocumentsPerPartition)
            skipValues.map(i => connector.withCollectionDo(readConfig, { coll: MongoCollection[BsonDocument] =>
              i >= count match {
                case true => None
                case false =>
                  Option(coll.find().skip(i).limit(-1).projection(Projections.include(partitionKey)).first()).map(_.get(partitionKey))
              }
            })).collect({ case Some(boundary) => boundary })
        }
        PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, PartitionerHelper.locations(connector))
      case Failure(ex: MongoCommandException) if ex.getErrorMessage.endsWith("not found.") =>
        logInfo(s"Could not find collection (${readConfig.collectionName}), using a single partition")
        MongoSinglePartitioner.partitions(connector, readConfig)
      case Failure(e) =>
        logWarning(s"Could not get collection statistics. Server errmsg: ${e.getMessage}")
        throw e
    }
  }

}

