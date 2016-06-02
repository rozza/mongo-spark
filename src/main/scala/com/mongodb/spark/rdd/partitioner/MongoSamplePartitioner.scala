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
 * *Note:* Requires MongoDB 3.2+
 *
 * @since 1.0
 */
case object MongoSamplePartitioner extends MongoPartitioner {

  private val samplesPerPartition = 10

  /**
   * Calculate the Partitions
   *
   * @param connector  the MongoConnector
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @return the partitions
   */
  override def partitions(connector: MongoConnector, readConfig: ReadConfig): Array[MongoPartition] = {
    require(readConfig.partitionKey == "_id", "The MongoSamplePartitioner can only partition on `_id`")

    Try(PartitionerHelper.collStats(connector, readConfig)) match {
      case Success(results) =>
        val partitionKey = readConfig.partitionKey
        val partitionSizeInBytes = readConfig.partitionSizeMB * 1024 * 1024
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
        MongoSinglePartitioner.partitions(connector, readConfig)
      case Failure(e) =>
        logWarning(s"Could not get collection statistics. Server errmsg: ${e.getMessage}")
        throw e
    }
  }
}
