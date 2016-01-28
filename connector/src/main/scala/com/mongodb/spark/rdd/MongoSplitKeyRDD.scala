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

package com.mongodb.spark.rdd

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Partition, SparkContext, TaskContext}

import org.bson.Document
import org.bson.conversions.Bson
import com.mongodb.client.MongoCursor
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.conf.ReadConfig
import com.mongodb.spark.rdd.partitioner.{MongoPartition, MongoSplitKeyPartitioner}

private[spark] object MongoSplitKeyRDD {

  def apply[D: ClassTag](sc: SparkContext, connector: MongoConnector, readConf: ReadConfig, pipeline: List[Bson] = List()): MongoSplitKeyRDD[D] = {
    val sharedConnector: Broadcast[MongoConnector] = sc.broadcast(connector)
    new MongoSplitKeyRDD[D](sc, sharedConnector, readConf, pipeline)
  }

}

private[rdd] class MongoSplitKeyRDD[D](
  @transient val sc: SparkContext,
  val connector:     Broadcast[MongoConnector],
  val readConfig:    ReadConfig,
  val pipeline:      Seq[Bson]
)(implicit val classTag: ClassTag[D])
    extends MongoRDD[D](sc, Nil) {

  override type Self = MongoSplitKeyRDD[D]

  override val mongoPartitioner = MongoSplitKeyPartitioner(readConfig)

  override def copy(
    connector:  Broadcast[MongoConnector] = connector,
    readConfig: ReadConfig                = readConfig,
    pipeline:   Seq[Bson]                 = pipeline
  ): Self = {

    checkSparkContext()
    new MongoSplitKeyRDD[D](
      sc = sc,
      connector = connector,
      readConfig = readConfig,
      pipeline = pipeline
    )
  }

  override protected def getPartitions: Array[Partition] = {
    checkSparkContext()
    mongoPartitioner.partitions(connector.value)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[D] = {
    val cursor: MongoCursor[D] = getCursor(split.asInstanceOf[MongoPartition])
    context.addTaskCompletionListener((ctx: TaskContext) => {
      log.debug("Task completed closing the cursor")
      cursor.close()
    })
    cursor.asScala
  }

  /**
   * Retrieves the partition's data from the collection based on the bounds of the partition.
   *
   * @return the cursor
   */
  private def getCursor(partition: MongoPartition): MongoCursor[D] = {
    val partitionPipeline: Seq[Bson] = new Document("$match", partition.queryBounds) +: pipeline
    connector.value.collection[D](readConfig.databaseName, readConfig.collectionName).aggregate(partitionPipeline.asJava).iterator
  }

}