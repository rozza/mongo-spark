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

package com.mongodb.spark.config

import java.util.concurrent.TimeUnit

import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import org.scalatest.{FlatSpec, Matchers}

import org.apache.spark.SparkConf

import com.mongodb.WriteConcern

class WriteConfigSpec extends FlatSpec with Matchers {

  "WriteConfig" should "have the expected defaults" in {
    val expectedWriteConfig = WriteConfig("db", "collection", WriteConcern.ACKNOWLEDGED)

    WriteConfig("db", "collection") should equal(expectedWriteConfig)
  }

  it should "be creatable from SparkConfig" in {
    forAll(writeConcerns) { writeConcern: WriteConcern =>
      val expectedWriteConfig = WriteConfig("db", "collection", writeConcern)

      val conf = sparkConf.clone()
      Option(writeConcern.getWObject).map(w => conf.set(s"${WriteConfig.configPrefix}${WriteConfig.writeConcernWProperty}", w.toString))
      Option(writeConcern.getJournal).map(j => conf.set(s"${WriteConfig.configPrefix}${WriteConfig.writeConcernJournalProperty}", j.toString))
      Option(writeConcern.getWTimeout(TimeUnit.MILLISECONDS)).map(t =>
        conf.set(s"${WriteConfig.configPrefix}${WriteConfig.writeConcernWTimeoutMSProperty}", t.toString))

      WriteConfig(conf) should equal(expectedWriteConfig)
    }
  }

  it should "round trip options" in {
    val defaultWriteConfig = WriteConfig("dbName", "collName", WriteConcern.ACKNOWLEDGED)
    forAll(writeConcerns) { writeConcern: WriteConcern =>
      val expectedWriteConfig = WriteConfig("db", "collection", writeConcern)
      defaultWriteConfig.withOptions(expectedWriteConfig.asOptions) should equal(expectedWriteConfig)
    }
  }

  it should "validate the values" in {
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.clone().remove("spark.mongodb.output.databaseName"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.clone().remove("spark.mongodb.output.collectionName"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.clone().set("spark.mongodb.output.writeConcern.w", "-1"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.clone().set("spark.mongodb.output.writeConcern.wTimeoutMS", "-1"))
  }

  val sparkConf = new SparkConf()
    .set("spark.mongodb.output.databaseName", "db")
    .set("spark.mongodb.output.collectionName", "collection")

  val writeConcerns = Table(
    "writeConcern",
    WriteConcern.UNACKNOWLEDGED,
    WriteConcern.ACKNOWLEDGED,
    WriteConcern.W1,
    WriteConcern.MAJORITY,
    WriteConcern.W1.withJournal(true),
    WriteConcern.W1.withJournal(true).withWTimeout(1, TimeUnit.MINUTES)
  )
}
