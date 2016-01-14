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
import scala.util.{Failure, Success, Try}

import org.bson.Document
import org.bson.types.{MaxKey, MinKey}
import com.mongodb.MongoNotPrimaryException
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.exceptions.MongoSplitException

private[partitioner] case class MongoStandaloneSplitter(connector: MongoConnector, splitKey: String, maxChunkSize: Int) extends MongoSplitter {

  override def bounds(): Seq[Document] = {
    val collection: MongoCollection[Document] = connector.collection()
    val ns: String = collection.getNamespace.getFullName
    logDebug(s"Getting split bounds for a non-sharded collection: $ns")

    val keyPattern: Document = new Document(splitKey, 1)
    val splitVectorCommand: Document = new Document("splitVector", ns)
      .append("keyPattern", keyPattern)
      .append("maxChunkSize", maxChunkSize)

    Try(connector.getDatabase().runCommand(splitVectorCommand)) match {
      case Success(result: Document) => createSplits(result)
      case Failure(e: MongoNotPrimaryException) => {
        logInfo(s"Splitting failed: '${e.getMessage}'. Continuing with a single partition.")
        createSplits(new Document("ok", 1.0))
      }
      case Failure(t: Throwable) => throw t
    }
  }

  private def createSplits(result: Document): Seq[Document] = {
    result.getDouble("ok").asInstanceOf[Double] match {
      case 1.0 =>
        val splitKeys: Seq[Document] = result.get("splitKeys").asInstanceOf[java.util.List[Document]].asScala
        if (splitKeys.isEmpty) {
          logInfo(
            """No splitKeys were calculated by the splitVector command, proceeding with a single partition.
              |If this is undesirable try lowering 'maxChunkSize' to produce more partitions.""".stripMargin.replaceAll("\n", " ")
          )
        }
        val minToMaxSplitKeys: Seq[Document] = new Document(splitKey, new MinKey) +: splitKeys :+ new Document(splitKey, new MaxKey)
        val splitKeyPairs: Seq[(Document, Document)] = minToMaxSplitKeys zip minToMaxSplitKeys.tail
        splitKeyPairs.map(x => createBoundaryQuery(splitKey, x._1.get(splitKey), x._2.get(splitKey)))
      case _ => throw new MongoSplitException(s"""Could not calculate standalone splits. Server errmsg: ${result.get("errmsg")}""")
    }
  }
}
