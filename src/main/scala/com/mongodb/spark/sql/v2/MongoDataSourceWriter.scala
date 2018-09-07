/*
 * Copyright 2018 MongoDB, Inc.
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

package com.mongodb.spark.sql.v2

import com.mongodb.MongoClient
import com.mongodb.client.model.{InsertOneModel, ReplaceOneModel, ReplaceOptions, UpdateOneModel, UpdateOptions, WriteModel}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.sql.MapFunctions.rowToDocumentMapper
import com.mongodb.spark.sql.v2.MongoDataSourceWriter.SaveType
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SaveMode}
import org.bson.BsonDocument

import scala.collection.JavaConverters._
import scala.collection.mutable

object MongoDataSourceWriter {
  object SaveType extends Enumeration {
    type SaveType = Value
    val Insert, Update, Replace = Value
  }
}

case class MongoDataSourceWriter(private val writeConfig: WriteConfig, private val schema: StructType, private val mode: SaveMode) extends DataSourceWriter {
  private val mapper = rowToDocumentMapper(schema)
  private val fieldNames = schema.fieldNames.toList
  private val queryKeyList = BsonDocument.parse(writeConfig.shardKey.getOrElse("{_id: 1}")).keySet().asScala.toList
  private val containsQueryKeyList = queryKeyList.forall(fieldNames.contains)
  private val saveType = {
    if (mode == SaveMode.Overwrite || writeConfig.forceInsert) {
      SaveType.Insert
    } else if (containsQueryKeyList) {
      if (writeConfig.replaceDocument) SaveType.Replace else SaveType.Update
    } else {
      SaveType.Insert
    }
  }
  val mongoConnector = MongoConnector(writeConfig.asOptions)

  override def createWriterFactory(): DataWriterFactory[Row] = MongoDataWriteFactory()

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
  }

  private case class MongoDataWriteFactory() extends DataWriterFactory[Row] {
    override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = MongoDataWriter(partitionId, attemptNumber)
  }

  private case class MongoDataWriter(partitionId: Int, attemptNumber: Int) extends DataWriter[Row] {
    @transient private val batch: mutable.Queue[WriteModel[BsonDocument]] = mutable.Queue.empty[WriteModel[BsonDocument]]
    @transient private var mongoClient: Option[MongoClient] = None

    override def write(record: Row): Unit = {
      val doc = mapper(record)
      val op = if (saveType == SaveType.Insert || !queryKeyList.forall(doc.containsKey)) {
        new InsertOneModel[BsonDocument](doc)
      } else {
        val queryDocument = new BsonDocument()
        queryKeyList.foreach(key => queryDocument.append(key, doc.get(key)))
        if (saveType == SaveType.Replace) {
          new ReplaceOneModel[BsonDocument](queryDocument, doc, new ReplaceOptions().upsert(true))
        } else {
          queryDocument.keySet().asScala.foreach(doc.remove(_))
          new UpdateOneModel[BsonDocument](queryDocument, new BsonDocument("$set", doc), new UpdateOptions().upsert(true))
        }
      }
      writeOperation(op)
    }

    override def commit(): WriterCommitMessage = {
      writeBatch(batch.dequeueAll({ _ => true }).toList)
      releaseClient()
      MongoWriterCommitMessage(partitionId, attemptNumber)
    }

    override def abort(): Unit = releaseClient()

    private def writeOperation(op: WriteModel[BsonDocument]): Unit = this.synchronized {
      batch += op
      if (batch.size >= writeConfig.maxBatchSize) {
        writeBatch(batch.dequeueAll({ _ => true }).toList)
      }
    }

    private def writeBatch(batch: List[WriteModel[BsonDocument]]): Unit = {
      if (mongoClient.isEmpty) {
        mongoClient = Some(mongoConnector.acquireClient())
      }
      mongoClient.map(c => c.getDatabase(writeConfig.databaseName).getCollection(writeConfig.collectionName, classOf[BsonDocument])
        .bulkWrite(batch.asJava))
    }

    private def releaseClient(): Unit = this.synchronized {
      if (mongoClient.isDefined) {
        mongoClient.foreach(mongoConnector.releaseClient)
        mongoClient = None
      }
    }
  }

  private case class MongoWriterCommitMessage(partitionId: Int, attemptNumber: Int) extends WriterCommitMessage
}
