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

import static java.util.Arrays.asList;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import org.bson.BsonRegularExpression;

public class BsonRegularExpressionDataType extends BsonDataType<BsonRegularExpression> {

  public static final BsonRegularExpressionDataType DATA_TYPE = new BsonRegularExpressionDataType();

  @Override
  public List<Object> getData(final BsonRegularExpression bsonValue) {
    return asList(bsonValue.getPattern(), bsonValue.getOptions());
  }

  @Override
  public List<StructField> getFieldList() {
    return asList(
        DataTypes.createStructField("regex", DataTypes.StringType, true),
        DataTypes.createStructField("options", DataTypes.StringType, true));
  }

  @Override
  public BsonRegularExpression fromSparkData(final Row row) {
    return new BsonRegularExpression(row.getString(0), row.getString(1));
  }

  private BsonRegularExpressionDataType() {}
}
