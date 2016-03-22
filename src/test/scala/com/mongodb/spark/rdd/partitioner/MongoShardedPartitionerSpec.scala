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

import com.mongodb.spark.RequiresMongoDB
import org.bson.{BsonDocument, BsonMaxKey, BsonMinKey, Document}

class MongoShardedPartitionerSpec extends RequiresMongoDB {

  "MongoShardedPartitioner" should "partition the database as expected" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    loadSampleDataIntoShardedCollection(5) // scalastyle:ignore

    val partitions = MongoShardedPartitioner.partitions(mongoConnector, readConfig)
    partitions.length should be >= 2
    partitions.head.locations should not be empty
  }

  it should "fallback to a single partition for a non sharded collections" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    loadSampleData(5) // scalastyle:ignore

    MongoShardedPartitioner.partitions(mongoConnector, readConfig).length should equal(1)
  }

  it should "have a default bounds of min to max key" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    val expectedBounds: BsonDocument = new BsonDocument(readConfig.splitKey, new BsonDocument("$gte", new BsonMinKey).append("$lt", new BsonMaxKey))
    shardCollection()
    collection.insertOne(new Document())

    MongoShardedPartitioner.partitions(mongoConnector, readConfig)(0).queryBounds should equal(expectedBounds)
  }

  it should "calculate the expected hosts for a single node shard" in {
    MongoShardedPartitioner.getHosts("sh0.example.com:27018") should contain theSameElementsInOrderAs Seq("sh0.example.com")
  }

  it should "calculate the expected hosts for a multi node shard" in {
    val hosts = MongoShardedPartitioner.getHosts("tic/sh0.rs1.example.com:27018,sh0.rs2.example.com:27018,sh0.rs1.example.com:27020")
    hosts should contain theSameElementsInOrderAs Seq("sh0.rs1.example.com", "sh0.rs2.example.com")
  }

}
