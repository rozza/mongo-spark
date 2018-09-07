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

import java.util.Optional

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SaveMode.{ErrorIfExists, Ignore, Overwrite}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, ReadSupportWithSchema, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.bson.{BsonArray, BsonDocument, BsonType, Document}

import scala.collection.JavaConverters._

class DefaultSource extends DataSourceV2 with ReadSupport with ReadSupportWithSchema {

  override def createReader(options: DataSourceOptions): DataSourceReader = createReader(None, options)

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = createReader(Some(schema), options)

  private def createReader(schema: Option[StructType], options: DataSourceOptions): DataSourceReader = {
    MongoDataSourceReader(schema, ReadConfig(options.asMap().asScala.toMap, None))
  }
}
