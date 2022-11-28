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

import static java.util.Collections.singletonList;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import org.bson.BsonJavaScript;

public class BsonJavaScriptDataType extends BsonDataType<BsonJavaScript> {

  public static final BsonJavaScriptDataType DATA_TYPE = new BsonJavaScriptDataType();

  @Override
  public List<Object> getData(final BsonJavaScript bsonValue) {
    return singletonList(bsonValue.getCode());
  }

  @Override
  public List<StructField> getFieldList() {
    return singletonList(DataTypes.createStructField("code", DataTypes.StringType, true));
  }

  @Override
  public BsonJavaScript fromSparkData(final Row row) {
    return new BsonJavaScript(row.getString(0));
  }

  private BsonJavaScriptDataType() {}
}
