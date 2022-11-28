/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb.spark.sql.connector.schema.compatibility;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.bson.BsonValue;

interface BsonCompatibility<T extends BsonValue> {

  List<Object> getData(T bsonValue);

  List<StructField> getFieldList();

  T fromSparkData(Row row);

  default StructType getSchema() {
    return DataTypes.createStructType(getFieldList());
  }

  default GenericRowWithSchema toSparkData(T bsonValue) {
    return new GenericRowWithSchema(getData(bsonValue).toArray(), getSchema());
  }
}
