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
import org.bson.BsonDocument;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class BsonJavaScriptWithScopeDataType extends BsonDataType<BsonJavaScriptWithScope> {

    public static final BsonJavaScriptWithScopeDataType DATA_TYPE = new BsonJavaScriptWithScopeDataType();

    @Override
    public List<Object> getData(final BsonJavaScriptWithScope bsonValue) {
        return asList(bsonValue.getCode(), bsonValue.getScope().toJson());
    }

    @Override
    public List<StructField> getFieldList() {
        return asList(      DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("scope", DataTypes.StringType, true));
    }

    @Override
    public BsonJavaScriptWithScope fromSparkData(final Row row) {
        return new BsonJavaScriptWithScope(row.getString(0), BsonDocument.parse(row.getString(1)));
    }

    private BsonJavaScriptWithScopeDataType(){}
}
