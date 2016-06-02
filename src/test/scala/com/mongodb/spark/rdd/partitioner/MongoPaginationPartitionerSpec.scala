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

import org.bson._
import com.mongodb.spark.{MongoConnector, RequiresMongoDB}

class MongoPaginationPartitionerSpec extends RequiresMongoDB {

  // scalastyle:off magic.number
  "MongoPaginationPartitioner" should "partition the database as expected" in {
    loadSampleData(10)

    val rightHandBoundaries = (1 to 100 by 10).map(x => new BsonString(f"$x%05d"))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val expectedPartitions = PartitionerHelper.createPartitions(readConfig.partitionKey, rightHandBoundaries, locations)
    val partitions = MongoPaginationPartitioner.partitions(mongoConnector, readConfig.copy(partitionSizeMB = 1))

    partitions should equal(expectedPartitions)

    val singlePartition = PartitionerHelper.createPartitions(readConfig.partitionKey, Seq.empty[BsonValue], locations)
    MongoPaginationPartitioner.partitions(mongoConnector, readConfig.copy(partitionSizeMB = 10)) should equal(singlePartition)
  }
  // scalastyle:on magic.number

  it should "have a default bounds of min to max key" in {
    val expectedBounds: BsonDocument = PartitionerHelper.createBoundaryQuery(readConfig.partitionKey, new BsonMinKey, new BsonMaxKey)
    collection.insertOne(new Document())
    MongoPaginationPartitioner.partitions(mongoConnector, readConfig)(0).queryBounds should equal(expectedBounds)
  }

  it should "handle no collection" in {
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig)
    MongoPaginationPartitioner.partitions(mongoConnector, readConfig) should equal(expectedPartitions)
  }
}

