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

import org.bson.BsonBoolean
import com.mongodb.MongoCommandException
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig

/**
 * The default collection partitioner implementation
 *
 * Checks if the collection is sharded then:
 *  - If sharded uses the [[MongoShardedPartitioner]] to partition the collection by shard chunks
 *  - If non sharded uses the [[MongoSplitVectorPartitioner]] to partition the collection by using the `splitVector` command.
 *
 * @since 1.0
 */
case object DefaultMongoPartitioner extends MongoPartitioner {

  override def partitions(connector: MongoConnector, readConfig: ReadConfig): Array[MongoPartition] = {
    val partitioner = Try(PartitionerHelper.collStats(connector, readConfig)) match {
      case Success(stats) => stats.getBoolean("sharded", new BsonBoolean(false)).getValue match {
        case true => MongoShardedPartitioner
        case false if connector.hasSampleAggregateOperator(readConfig) => MongoSamplePartitioner
        case false => MongoPaginationPartitioner
      }
      case Failure(ex: MongoCommandException) if ex.getErrorMessage.endsWith("not found.") =>
        logInfo(s"Could not find collection (${readConfig.collectionName}), using single partition")
        MongoSinglePartitioner
      case Failure(e) =>
        logWarning(s"Could not get collection statistics. Server errmsg: ${e.getMessage}")
        throw e
    }
    partitioner.partitions(connector, readConfig)
  }

}
