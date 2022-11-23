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
import org.bson.BsonSymbol;
import org.bson.types.ObjectId;

import java.util.List;

import static java.util.Collections.singletonList;

public class BsonSymbolDataType extends BsonDataType<BsonSymbol> {

    public static final BsonSymbolDataType DATA_TYPE = new BsonSymbolDataType();

    @Override
    public List<Object> getData(final BsonSymbol bsonValue) {
        return singletonList(bsonValue.getSymbol());
    }

    @Override
    public List<StructField> getFieldList() {
        return singletonList(DataTypes.createStructField("symbol", DataTypes.StringType, true));
    }

    @Override
    public BsonSymbol fromSparkData(final Row row) {
        return  new BsonSymbol(row.getString(0));
    }

    private BsonSymbolDataType(){}
}
