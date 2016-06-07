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

import org.bson.{BsonDocument, BsonValue}
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Projections, Sorts}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig

private[partitioner] trait MongoPaginationPartitioner {

  private implicit object BsonValueOrdering extends BsonValueOrdering

  def calculateSkipPartitions(connector: MongoConnector, readConfig: ReadConfig, partitionKey: String, count: Long, skipValues: Seq[Int]): Seq[BsonValue] = {
    skipValues.map(i => connector.withCollectionDo(readConfig, { coll: MongoCollection[BsonDocument] =>
      i >= count match {
        case true => None
        case false =>
          Option(coll.find()
            .skip(i)
            .limit(-1)
            .projection(Projections.include(partitionKey))
            .sort(Sorts.ascending(partitionKey))
            .first())
            .map(_.get(partitionKey))
      }
    })).collect({ case Some(boundary) => boundary }).sorted
  }

}
