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

import scala.collection.JavaConverters._

import org.bson.Document
import org.bson.types.{MaxKey, MinKey}
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Filters, Projections}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.conf.ReadConfig

private[partitioner] case class MongoShardedSplitter(connector: MongoConnector, readConf: ReadConfig) extends MongoSplitter {

  override def bounds(): Seq[Document] = {
    val collection: MongoCollection[Document] = connector.collection(readConf.databaseName, readConf.collectionName)
    val ns: String = collection.getNamespace.getFullName
    logDebug(s"Getting split bounds for a sharded collection: $ns")

    val chunks: Seq[Document] = connector.collection("config", "chunks")
      .find(Filters.eq("ns", ns)).projection(Projections.include("min", "max"))
      .into(new java.util.ArrayList[Document]).asScala

    chunks.isEmpty match {
      case true =>
        logWarning(
          s"""Collection '$ns' is not sharded.
             |Continuing with a single partition.
             |To split the collections into multiple partitions connect to the MongoDB node directly""".stripMargin.replaceAll("\n", " ")
        )
        Seq(createBoundaryQuery(readConf.splitKey, new MinKey, new MaxKey))
      case false => chunks.map(x => createBoundaryQuery(
        readConf.splitKey,
        x.get[Document]("min", classOf[Document]).get(readConf.splitKey),
        x.get[Document]("max", classOf[Document]).get(readConf.splitKey)
      ))
    }
  }
}
