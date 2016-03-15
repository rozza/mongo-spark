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

/**
 * Mongo output configurations
 *
 * Configurations used when writing data from Spark into MongoDB
 *
 * outputProperties
 *
 * @see [[WriteConfig]]
 * @since 1.0
 *
 * @define outputProperties
 *
 * == Configuration Properties ==
 *
 * The prefix when using `sparkConf` is: `spark.mongodb.output.` followed by the property name:
 *
 *  - [[databaseNameProperty databaseName]], the database name to write data to.
 *  - [[collectionNameProperty collectionName]], the collection name to write data to.
 *  - [[writeConcernWProperty writeConcern.w]], the write concern w value.
 *  - [[writeConcernJournalProperty writeConcern.journal]], the write concern journal value.
 *  - [[writeConcernWTimeoutMSProperty writeConcern.wTimeoutMS]], the write concern wTimeout value.
 *
 */
trait MongoOutputConfig extends MongoConfig with Serializable {

  override val configPrefix = "spark.mongodb.output."

  /**
   * The database name property
   */
  val databaseNameProperty = "databaseName".toLowerCase

  /**
   * The collection name property
   */
  val collectionNameProperty = "collectionName".toLowerCase

  /**
   * The `WriteConcern` w property
   *
   * @see [[WriteConcernConfig]]
   */
  val writeConcernWProperty = "writeConcern.w".toLowerCase

  /**
   * The `WriteConcern` journal property
   *
   * @see [[WriteConcernConfig]]
   */
  val writeConcernJournalProperty = "writeConcern.journal".toLowerCase

  /**
   * The `WriteConcern` wTimeoutMS property
   *
   * @see [[WriteConcernConfig]]
   */
  val writeConcernWTimeoutMSProperty = "writeConcern.wTimeoutMS".toLowerCase
}
