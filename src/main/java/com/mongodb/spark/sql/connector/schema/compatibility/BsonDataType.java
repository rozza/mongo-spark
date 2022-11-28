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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.bson.BsonValue;

public abstract class BsonDataType<T extends BsonValue> extends DataType
    implements BsonCompatibility<T> {
  BsonDataType() {}

  @Override
  public int defaultSize() {
    return getFieldList().stream().map(f -> f.dataType().defaultSize()).mapToInt(i -> i).sum();
  }

  @Override
  public DataType asNullable() {
    return this;
  }

  public boolean isSameType(final StructType dataType) {
    List<StructField> fieldList = getFieldList();

    return dataType.fields().length == fieldList.size()
        && Arrays.stream(dataType.fieldNames())
            .collect(Collectors.toSet())
            .equals(fieldList.stream().map(StructField::name).collect(Collectors.toSet()))
        && Arrays.stream(dataType.fields())
            .map(StructField::dataType)
            .collect(Collectors.toSet())
            .equals(fieldList.stream().map(StructField::dataType).collect(Collectors.toSet()));
  }
}
