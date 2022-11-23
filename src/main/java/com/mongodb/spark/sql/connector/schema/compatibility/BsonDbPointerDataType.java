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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.bson.BsonDbPointer;
import org.bson.types.ObjectId;

import java.util.List;

import static java.util.Arrays.asList;

public class BsonDbPointerDataType extends BsonDataType<BsonDbPointer> {

    public static final BsonDbPointerDataType DATA_TYPE = new BsonDbPointerDataType();

    @Override
    public List<Object> getData(final BsonDbPointer bsonValue) {
        return asList(bsonValue.getNamespace(), bsonValue.getId().toHexString());
    }

    @Override
    public List<StructField> getFieldList() {
        return asList(
                DataTypes.createStructField("ref", DataTypes.StringType, true),
                DataTypes.createStructField("oid", DataTypes.StringType, true));
    }

    @Override
    public BsonDbPointer fromSparkData(final Row row) {
        return new BsonDbPointer(row.getString(0), new ObjectId(row.getString(1)));
    }

    private BsonDbPointerDataType(){}
}
