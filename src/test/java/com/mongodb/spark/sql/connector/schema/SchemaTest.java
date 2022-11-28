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

package com.mongodb.spark.sql.connector.schema;

import static com.mongodb.spark.sql.connector.schema.InferSchema.PLACE_HOLDER_ARRAY_TYPE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

import com.mongodb.spark.sql.connector.schema.compatibility.BsonBinaryDataType;
import com.mongodb.spark.sql.connector.schema.compatibility.BsonJavaScriptDataType;
import com.mongodb.spark.sql.connector.schema.compatibility.BsonJavaScriptWithScopeDataType;
import com.mongodb.spark.sql.connector.schema.compatibility.BsonMaxKeyDataType;
import com.mongodb.spark.sql.connector.schema.compatibility.BsonMinKeyDataType;
import com.mongodb.spark.sql.connector.schema.compatibility.BsonObjectIdDataType;
import com.mongodb.spark.sql.connector.schema.compatibility.BsonRegularExpressionDataType;
import com.mongodb.spark.sql.connector.schema.compatibility.BsonSymbolDataType;
import com.mongodb.spark.sql.connector.schema.compatibility.BsonTimestampDataType;
import com.mongodb.spark.sql.connector.schema.compatibility.BsonUndefinedDataType;

abstract class SchemaTest {

  static final BsonDocument SIMPLE_BSON_DOCUMENT =
      BsonDocument.parse(
          "{"
              + "'binaryType': {'$binary': {'base64': 'YWJj', 'subType': '00'}}, "
              + "'booleanType': true, "
              + "'byteType': 1, "
              + "'dateType': {'$date': '1970-01-01T01:00:00Z'}, "
              + "'doubleType': 2.0, "
              + "'floatType': 3.0, "
              + "'integerType': 5, "
              + "'longType': {'$numberLong': '6'}, "
              + "'shortType': 7, "
              + "'stringType': 'string', "
              + "'timestampType': {'$date': '1970-01-01T05:00:00Z'}"
              + "}");

  static final Row SIMPLE_ROW =
      new GenericRowWithSchema(
          asList(
                  "abc".getBytes(StandardCharsets.UTF_8),
                  true,
                  (byte) 1,
                  new Timestamp(3600000L),
                  2.0,
                  3.0f,
                  5,
                  6L,
                  null,
                  (short) 7,
                  "string",
                  new Date(18000000L))
              .toArray(),
          new StructType()
              .add("binaryType", DataTypes.BinaryType, true)
              .add("booleanType", DataTypes.BooleanType, true)
              .add("byteType", DataTypes.ByteType, true)
              .add("dateType", DataTypes.DateType, true)
              .add("doubleType", DataTypes.DoubleType, true)
              .add("floatType", DataTypes.FloatType, true)
              .add("integerType", DataTypes.IntegerType, true)
              .add("longType", DataTypes.LongType, true)
              .add("nullType", DataTypes.NullType, true)
              .add("shortType", DataTypes.ShortType, true)
              .add("stringType", DataTypes.StringType, true)
              .add("timestampType", DataTypes.TimestampType, true));

  static final String SUB_BSON_DOCUMENT_JSON =
      "{\"A\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}},"
          + " \"B\": {\"$date\": {\"$numberLong\": \"1577863627000\"}},"
          + " \"C\": {\"D\": \"12345.6789\"}}";
  static final String BSON_DOCUMENT_JSON =
      "{\"_id\": {\"$oid\": \"5f15aab12435743f9bd126a4\"},"
          + " \"myString\": \"some foo bla text\","
          + " \"myInt\": {\"$numberInt\": \"42\"},"
          + " \"myDouble\": {\"$numberDouble\": \"20.21\"},"
          + " \"mySubDoc\": "
          + SUB_BSON_DOCUMENT_JSON
          + ","
          + " \"myArray\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}],"
          + " \"myBytes\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}},"
          + " \"myDate\": {\"$date\": {\"$numberLong\": \"1234567890\"}},"
          + " \"myDecimal\": {\"$numberDecimal\": \"12345.6789\"}"
          + "}";
  static final BsonDocument BSON_DOCUMENT = RawBsonDocument.parse(BSON_DOCUMENT_JSON);

  static final String BSON_DOCUMENT_ALL_TYPES_JSON =
      "{"
          + "\"arrayEmpty\": [], "
          + "\"arraySimple\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}], "
          + "\"arrayComplex\": [{\"a\": {\"$numberInt\": \"1\"}}, {\"a\": {\"$numberInt\": \"2\"}}], "
          + "\"arrayMixedTypes\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, true,"
          + " [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}],"
          + " {\"a\": {\"$numberInt\": \"2\"}}], "
          + "\"arrayComplexMixedTypes\": [{\"a\": {\"$numberInt\": \"1\"}}, {\"a\": \"a\"}], "
          + "\"binary\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}}, "
          + "\"boolean\": true, "
          + "\"code\": {\"$code\": \"int i = 0;\"}, "
          + "\"codeWithScope\": {\"$code\": \"int x = y;\", \"$scope\": {\"y\": {\"$numberInt\": \"1\"}}}, "
          + "\"dateTime\": {\"$date\": {\"$numberLong\": \"1577836801000\"}}, "
          + "\"decimal128\": {\"$numberDecimal\": \"1.0\"}, "
          + "\"documentEmpty\": {},"
          + "\"document\": {\"a\": {\"$numberInt\": \"1\"}}, "
          + "\"double\": {\"$numberDouble\": \"62.0\"}, "
          + "\"int32\": {\"$numberInt\": \"42\"}, "
          + "\"int64\": {\"$numberLong\": \"52\"}, "
          + "\"maxKey\": {\"$maxKey\": 1}, "
          + "\"minKey\": {\"$minKey\": 1}, "
          + "\"null\": null, "
          + "\"objectId\": {\"$oid\": \"5f3d1bbde0ca4d2829c91e1d\"}, "
          + "\"regex\": {\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\"}}, "
          + "\"string\": \"the fox ...\", "
          + "\"symbol\": {\"$symbol\": \"ruby stuff\"}, "
          + "\"timestamp\": {\"$timestamp\": {\"t\": 305419896, \"i\": 5}}, "
          + "\"undefined\": {\"$undefined\": true}"
          + "}";

  static final BsonDocument BSON_DOCUMENT_ALL_TYPES =
      RawBsonDocument.parse(BSON_DOCUMENT_ALL_TYPES_JSON);

  static final BsonDocument BSON_DOCUMENT_ALL_EXTENDED_TYPES =
      BsonDocument.parse(BSON_DOCUMENT_ALL_TYPES_JSON)
          .append(
              "binary",
              new BsonBinary((byte) 0x80, BSON_DOCUMENT_ALL_TYPES.getBinary("binary").getData()));

  static final StructType BSON_DOCUMENT_ALL_TYPES_SCHEMA =
      new StructType()
          .add("arrayEmpty", PLACE_HOLDER_ARRAY_TYPE)
          .add("arraySimple", DataTypes.createArrayType(DataTypes.IntegerType, true))
          .add(
              "arrayComplex",
              DataTypes.createArrayType(
                  DataTypes.createStructType(
                      singletonList(
                          DataTypes.createStructField("a", DataTypes.IntegerType, true)))))
          .add("arrayMixedTypes", DataTypes.createArrayType(DataTypes.StringType, true))
          .add(
              "arrayComplexMixedTypes",
              DataTypes.createArrayType(
                  DataTypes.createStructType(
                      singletonList(DataTypes.createStructField("a", DataTypes.StringType, true)))))
          .add("binary", DataTypes.BinaryType)
          .add("boolean", DataTypes.BooleanType)
          .add("code", DataTypes.StringType)
          .add("codeWithScope", DataTypes.StringType)
          .add("dateTime", DataTypes.TimestampType)
          .add("decimal128", DataTypes.createDecimalType(2, 1))
          .add("documentEmpty", DataTypes.createStructType(emptyList()))
          .add(
              "document",
              DataTypes.createStructType(
                  singletonList(DataTypes.createStructField("a", DataTypes.IntegerType, true))))
          .add("double", DataTypes.DoubleType)
          .add("int32", DataTypes.IntegerType)
          .add("int64", DataTypes.LongType)
          .add("maxKey", DataTypes.StringType)
          .add("minKey", DataTypes.StringType)
          .add("null", DataTypes.NullType)
          .add("objectId", DataTypes.StringType)
          .add("regex", DataTypes.StringType)
          .add("string", DataTypes.StringType)
          .add("symbol", DataTypes.StringType)
          .add("timestamp", DataTypes.TimestampType)
          .add("undefined", DataTypes.StringType);

  static final StructType BSON_DOCUMENT_ALL_TYPES_SCHEMA_FINAL =
      DataTypes.createStructType(
          Arrays.stream(BSON_DOCUMENT_ALL_TYPES_SCHEMA.fields())
              .map(
                  f -> {
                    if (f.dataType().sameType(PLACE_HOLDER_ARRAY_TYPE)) {
                      return DataTypes.createStructField(
                          f.name(),
                          DataTypes.createArrayType(DataTypes.StringType, true),
                          f.nullable());
                    }
                    return f;
                  })
              .collect(Collectors.toList()));

  static final GenericRowWithSchema ALL_TYPES_ROW =
      new GenericRowWithSchema(
          asList(
                  emptyList().toArray(),
                  asList(1, 2, 3).toArray(),
                  asList(
                          new GenericRowWithSchema(
                              singletonList(1).toArray(),
                              DataTypes.createStructType(
                                  singletonList(
                                      DataTypes.createStructField(
                                          "a", DataTypes.IntegerType, true)))),
                          new GenericRowWithSchema(
                              singletonList(2).toArray(),
                              DataTypes.createStructType(
                                  singletonList(
                                      DataTypes.createStructField(
                                          "a", DataTypes.IntegerType, true)))))
                      .toArray(),
                  asList("1", "2", "true", "[1, 2, 3]", "{\"a\": 2}").toArray(),
                  asList(
                          new GenericRowWithSchema(
                              singletonList("1").toArray(),
                              DataTypes.createStructType(
                                  singletonList(
                                      DataTypes.createStructField(
                                          "a", DataTypes.StringType, true)))),
                          new GenericRowWithSchema(
                              singletonList("a").toArray(),
                              DataTypes.createStructType(
                                  singletonList(
                                      DataTypes.createStructField(
                                          "a", DataTypes.StringType, true)))))
                      .toArray(),
                  new byte[] {75, 97, 102, 107, 97, 32, 114, 111, 99, 107, 115, 33},
                  true,
                  "{\"$code\": \"int i = 0;\"}",
                  "{\"$code\": \"int x = y;\", \"$scope\": {\"y\": 1}}",
                  new Timestamp(1577836801000L),
                  new BigDecimal("1.0"),
                  new GenericRowWithSchema(
                      emptyList().toArray(), DataTypes.createStructType(emptyList())),
                  new GenericRowWithSchema(
                      singletonList(1).toArray(),
                      DataTypes.createStructType(
                          singletonList(
                              DataTypes.createStructField("a", DataTypes.IntegerType, true)))),
                  62.0,
                  42,
                  52L,
                  "{\"$maxKey\": 1}",
                  "{\"$minKey\": 1}",
                  null,
                  "5f3d1bbde0ca4d2829c91e1d",
                  "{\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\"}}",
                  "the fox ...",
                  "ruby stuff",
                  new Timestamp(305419896000L),
                  "{\"$undefined\": true}")
              .toArray(),
          BSON_DOCUMENT_ALL_TYPES_SCHEMA_FINAL);

  static final StructType BSON_DOCUMENT_EXTENDED_TYPES_SCHEMA =
      new StructType()
          .add("arrayEmpty", DataTypes.createArrayType(DataTypes.StringType, true))
          .add("arraySimple", DataTypes.createArrayType(DataTypes.IntegerType, true))
          .add(
              "arrayComplex",
              DataTypes.createArrayType(
                  DataTypes.createStructType(
                      singletonList(
                          DataTypes.createStructField("a", DataTypes.IntegerType, true)))))
          .add("arrayMixedTypes", DataTypes.createArrayType(DataTypes.StringType, true))
          .add(
              "arrayComplexMixedTypes",
              DataTypes.createArrayType(
                  DataTypes.createStructType(
                      singletonList(DataTypes.createStructField("a", DataTypes.StringType, true)))))
          .add("boolean", DataTypes.BooleanType)
          .add("dateTime", DataTypes.TimestampType)
          .add("decimal128", DataTypes.createDecimalType(2, 1))
          .add("documentEmpty", DataTypes.createStructType(emptyList()))
          .add(
              "document",
              DataTypes.createStructType(
                  singletonList(DataTypes.createStructField("a", DataTypes.IntegerType, true))))
          .add("double", DataTypes.DoubleType)
          .add("int32", DataTypes.IntegerType)
          .add("int64", DataTypes.LongType)
          .add("null", DataTypes.NullType)
          .add("string", DataTypes.StringType)
          .add("binary", BsonBinaryDataType.DATA_TYPE.getSchema())
          .add("code", BsonJavaScriptDataType.DATA_TYPE.getSchema())
          .add("codeWithScope", BsonJavaScriptWithScopeDataType.DATA_TYPE.getSchema())
          .add("maxKey", BsonMaxKeyDataType.DATA_TYPE.getSchema())
          .add("minKey", BsonMinKeyDataType.DATA_TYPE.getSchema())
          .add("objectId", BsonObjectIdDataType.DATA_TYPE.getSchema())
          .add("regex", BsonRegularExpressionDataType.DATA_TYPE.getSchema())
          .add("symbol", BsonSymbolDataType.DATA_TYPE.getSchema())
          .add("timestamp", BsonTimestampDataType.DATA_TYPE.getSchema())
          .add("undefined", BsonUndefinedDataType.DATA_TYPE.getSchema());

  static final GenericRowWithSchema ALL_TYPES_EXTENDED_ROW =
      new GenericRowWithSchema(
          asList(
                  emptyList().toArray(),
                  asList(1, 2, 3).toArray(),
                  asList(
                          new GenericRowWithSchema(
                              singletonList(1).toArray(),
                              DataTypes.createStructType(
                                  singletonList(
                                      DataTypes.createStructField(
                                          "a", DataTypes.IntegerType, true)))),
                          new GenericRowWithSchema(
                              singletonList(2).toArray(),
                              DataTypes.createStructType(
                                  singletonList(
                                      DataTypes.createStructField(
                                          "a", DataTypes.IntegerType, true)))))
                      .toArray(),
                  asList("1", "2", "true", "[1, 2, 3]", "{\"a\": 2}").toArray(),
                  asList(
                          new GenericRowWithSchema(
                              singletonList("1").toArray(),
                              DataTypes.createStructType(
                                  singletonList(
                                      DataTypes.createStructField(
                                          "a", DataTypes.StringType, true)))),
                          new GenericRowWithSchema(
                              singletonList("a").toArray(),
                              DataTypes.createStructType(
                                  singletonList(
                                      DataTypes.createStructField(
                                          "a", DataTypes.StringType, true)))))
                      .toArray(),
                  true,
                  new Timestamp(1577836801000L),
                  new BigDecimal("1.0"),
                  new GenericRowWithSchema(
                      emptyList().toArray(), DataTypes.createStructType(emptyList())),
                  new GenericRowWithSchema(
                      singletonList(1).toArray(),
                      DataTypes.createStructType(
                          singletonList(
                              DataTypes.createStructField("a", DataTypes.IntegerType, true)))),
                  62.0,
                  42,
                  52L,
                  null,
                  "the fox ...",
                  new GenericRowWithSchema(
                      asList(
                              (byte) 0x80,
                              new byte[] {75, 97, 102, 107, 97, 32, 114, 111, 99, 107, 115, 33})
                          .toArray(),
                      BsonBinaryDataType.DATA_TYPE.getSchema()),
                  new GenericRowWithSchema(
                      singletonList("int i = 0;").toArray(),
                      BsonJavaScriptDataType.DATA_TYPE.getSchema()),
                  new GenericRowWithSchema(
                      asList("int x = y;", "{\"y\": 1}").toArray(),
                      BsonJavaScriptWithScopeDataType.DATA_TYPE.getSchema()),
                  new GenericRowWithSchema(
                      singletonList(1).toArray(), BsonMaxKeyDataType.DATA_TYPE.getSchema()),
                  new GenericRowWithSchema(
                      singletonList(1).toArray(), BsonMinKeyDataType.DATA_TYPE.getSchema()),
                  new GenericRowWithSchema(
                      singletonList("5f3d1bbde0ca4d2829c91e1d").toArray(),
                      BsonObjectIdDataType.DATA_TYPE.getSchema()),
                  new GenericRowWithSchema(
                      asList("^test.*regex.*xyz$", "i").toArray(),
                      BsonRegularExpressionDataType.DATA_TYPE.getSchema()),
                  new GenericRowWithSchema(
                      singletonList("ruby stuff").toArray(),
                      BsonSymbolDataType.DATA_TYPE.getSchema()),
                  new GenericRowWithSchema(
                      asList(305419896, 5).toArray(), BsonTimestampDataType.DATA_TYPE.getSchema()),
                  new GenericRowWithSchema(
                      singletonList(true).toArray(), BsonUndefinedDataType.DATA_TYPE.getSchema()))
              .toArray(),
          BSON_DOCUMENT_EXTENDED_TYPES_SCHEMA);
}
