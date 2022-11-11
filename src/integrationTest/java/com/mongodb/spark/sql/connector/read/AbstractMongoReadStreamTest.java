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
package com.mongodb.spark.sql.connector.read;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import org.bson.BsonDocument;
import org.bson.BsonString;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;

abstract class AbstractMongoReadStreamTest extends MongoSparkConnectorTestCase {

  private static final String MEMORY = "memory";
  private static final String MONGODB = "mongodb";

  abstract String collectionPrefix();

  abstract Trigger getTrigger();

  private String testIdentifier;

  @Test
  void testStream() {
    assumeTrue(supportsChangeStreams());
    testIdentifier = "Simple";
    testStreamingQuery(
        createMongoConfig(),
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)),
        withSource("inserting 100-125", (msg, coll) -> coll.insertMany(createDocuments(100, 125))),
        withMemorySink(
            "Expecting to see 50 documents",
            (msg, ds) -> assertEquals(50, ds.collectAsList().size(), msg)));
  }

  @Test
  void testStreamHandlesCollectionDrop() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    testIdentifier = "WithDrop";
    testStreamingQuery(
        createMongoConfig(),
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)),
        withSource("Dropping Collection", (msg, coll) -> coll.drop()),
        withMemorySink(
            "Expected to see 1 drop document",
            (msg, ds) -> assertEquals(1, ds.filter("operationType == 'drop'").count(), msg)),
        withMemorySink(
            "Expected to see 1 invalidate document",
            (msg, ds) -> assertEquals(1, ds.filter("operationType == 'invalidate'").count(), msg)));
  }

  @Test
  void testStreamWithFilter() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    testIdentifier = "WithFilter";
    Column filterColumn = new Column("operationType").equalTo("insert");
    testStreamingQuery(
        createMongoConfig()
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_LOOKUP_FULL_DOCUMENT_CONFIG,
                "updateLookup"),
        filterColumn,
        withSource("inserting 0-50", (msg, coll) -> coll.insertMany(createDocuments(0, 50))),
        withMemorySink(
            "Expected to see 50 documents",
            (msg, ds) -> assertEquals(50, ds.collectAsList().size(), msg)),
        withSource(
            "Deleting documents",
            (msg, coll) ->
                coll.deleteMany(
                    Filters.in(
                        "_id",
                        IntStream.range(0, 50)
                            .filter(i -> i % 2 == 0)
                            .mapToObj(idFieldMapper())
                            .collect(Collectors.toList())))),
        withSource("Inserting 100-125", (msg, coll) -> coll.insertMany(createDocuments(100, 125))),
        withMemorySink(
            "Expected to see 75 documents",
            (msg, ds) -> assertEquals(75, ds.collectAsList().size(), msg)));
  }

  @Test
  void testStreamWithPublishFullDocumentOnly() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    testIdentifier = "FullDocOnly";
    testStreamingQuery(
        createMongoConfig()
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG,
                "true")
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_LOOKUP_FULL_DOCUMENT_CONFIG,
                "overwritten / ignored")
            .withOption(WriteConfig.WRITE_PREFIX + WriteConfig.OPERATION_TYPE_CONFIG, "Update"),
        createStructType(
            asList(
                createStructField("_id", DataTypes.StringType, false),
                createStructField("a", DataTypes.StringType, false))),
        withSource(
            "Inserting 0-50",
            (msg, coll) ->
                coll.insertMany(
                    createDocuments(
                        0,
                        50,
                        i ->
                            new BsonDocument("_id", new BsonString(testIdentifier + "-" + i))
                                .append("a", new BsonString("a"))))),
        withMemorySink(
            "Expected to see 50 documents",
            (msg, ds) -> assertEquals(50, ds.collectAsList().size(), msg)),
        withSource(
            "Updating evens",
            (msg, coll) ->
                coll.updateMany(
                    Filters.in(
                        "_id",
                        IntStream.range(0, 50)
                            .filter(i -> i % 2 == 0)
                            .mapToObj(idFieldMapper())
                            .collect(Collectors.toList())),
                    Updates.set("a", new BsonString("b")))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.filter("a == 'b'").count(), msg)));
  }

  @Test
  void testStreamPublishFullDocumentOnlyHandlesCollectionDrop() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    testIdentifier = "FullDocOnlyWithDrop";
    testStreamingQuery(
        createMongoConfig()
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG,
                "true")
            .withOption(WriteConfig.WRITE_PREFIX + WriteConfig.OPERATION_TYPE_CONFIG, "Update"),
        createStructType(singletonList(createStructField("_id", DataTypes.StringType, false))),
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)),
        withSource(
            "Dropping the collection",
            (msg, coll) -> coll.drop()), // Dropping collection shouldn't error
        withSource("Inserting 100-125", (msg, coll) -> coll.insertMany(createDocuments(100, 125))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)));
  }

  @Test
  void testStreamCustomMongoClientFactory() {
    assumeTrue(supportsChangeStreams());
    testIdentifier = "CustomClientFactory";
    testStreamingQuery(
        createMongoConfig()
            .withOption(
                ReadConfig.PREFIX + ReadConfig.CLIENT_FACTORY_CONFIG,
                "com.mongodb.spark.sql.connector.read.CustomMongoClientFactory"),
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)));

    assertTrue(CustomMongoClientFactory.CALLED.get());
  }

  @Test
  void testStreamNoSchema() {
    SparkSession spark = getOrCreateSparkSession();
    Throwable cause =
        assertThrows(
            Exception.class,
            () ->
                spark
                    .readStream()
                    .format(MONGODB)
                    .load()
                    .writeStream()
                    .trigger(getTrigger())
                    .format(MEMORY)
                    .queryName("test")
                    .outputMode("append")
                    .start()
                    .processAllAvailable());

    if (cause instanceof StreamingQueryException) {
      cause = ((StreamingQueryException) cause).cause();
    }
    assertTrue(cause instanceof ConfigException, format("Expected ConfigException: %s", cause));
  }

  @Test
  void testStreamWriteStream() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    testIdentifier = "RoundTrip";
    testStreamingQuery(
        "mongodb",
        createMongoConfig(),
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withSink(
            "Expected to see 25 documents",
            (msg, coll) -> assertEquals(25, coll.countDocuments(), msg)),
        withSource("Inserting 100-125", (msg, coll) -> coll.insertMany(createDocuments(100, 125))),
        withSink(
            "Expected to see 50 documents",
            (msg, coll) -> assertEquals(50, coll.countDocuments(), msg)));
  }

  private static final StructType DEFAULT_SCHEMA =
      createStructType(
          asList(
              createStructField("operationType", DataTypes.StringType, false),
              createStructField("clusterTime", DataTypes.StringType, false),
              createStructField("fullDocument", DataTypes.StringType, true)));

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {
    testStreamingQuery(MEMORY, mongoConfig, setup, consumers);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final String writeFormat,
      final MongoConfig mongoConfig,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {
    testStreamingQuery(writeFormat, mongoConfig, DEFAULT_SCHEMA, null, setup, consumers);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final StructType schema,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {
    testStreamingQuery(MEMORY, mongoConfig, schema, null, setup, consumers);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final Column condition,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {
    testStreamingQuery(MEMORY, mongoConfig, DEFAULT_SCHEMA, condition, setup, consumers);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final String writeFormat,
      final MongoConfig mongoConfig,
      final StructType schema,
      final Column condition,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {

    StreamingQuery streamingQuery =
        createStreamingQuery(writeFormat, mongoConfig, schema, condition);
    try {
      retryAssertion(
          () ->
              assertFalse(
                  streamingQuery.status().message().contains("Initializing"),
                  "Stream is not initialized"));
      try {
        setup.accept(mongoConfig);
      } catch (Exception e) {
        throw new AssertionFailedError("Setup failed: " + e.getMessage());
      }

      for (Consumer<MongoConfig> consumer : consumers) {
        retryAssertion(
            () -> consumer.accept(mongoConfig),
            () -> {
              mongoConfig
                  .toReadConfig()
                  .doWithCollection(
                      coll ->
                          LOGGER.info(
                              "Source Collection Status: {}.",
                              coll.find().map(BsonDocument::toJson).into(new ArrayList<>())));

              if (writeFormat.equals(MONGODB)) {
                mongoConfig
                    .toWriteConfig()
                    .doWithCollection(
                        coll ->
                            LOGGER.info(
                                "Sink Collection Status: {}.",
                                coll.find().map(BsonDocument::toJson).into(new ArrayList<>())));
              } else {
                LOGGER.info(
                    "Memory Sink Status: {}.",
                    getOrCreateSparkSession()
                        .sql("select * from " + testIdentifier)
                        .collectAsList()
                        .stream()
                        .map(
                            r ->
                                Arrays.stream(r.schema().fields())
                                    .map(f -> f.name() + ": " + r.get(r.fieldIndex(f.name())))
                                    .collect(Collectors.joining(", ", "{", "}")))
                        .collect(Collectors.joining(", ", "[", "]")));
              }
            });
      }
    } catch (RuntimeException e) {
      fail("Assertions caused an exception", e);
    } finally {
      try {
        streamingQuery.stop();
      } catch (TimeoutException e) {
        fail("Stopping the stream failed: ", e);
      }
    }
  }

  private StreamingQuery createStreamingQuery(
      final String writeFormat,
      final MongoConfig mongoConfig,
      final StructType schema,
      final Column condition) {
    Dataset<Row> ds =
        getOrCreateSparkSession(getSparkConf().set("numPartitions", "1"))
            .readStream()
            .format(MONGODB)
            .options(mongoConfig.toReadConfig().getOptions())
            .schema(schema)
            .load();

    if (condition != null) {
      ds = ds.filter(condition);
    }

    try {
      return ds.writeStream()
          .format(writeFormat)
          .queryName(testIdentifier)
          .trigger(getTrigger())
          .start();
    } catch (TimeoutException e) {
      return fail(e);
    }
  }

  private Consumer<MongoConfig> withSource(
      final String msg, final BiConsumer<String, MongoCollection<BsonDocument>> biConsumer) {
    LOGGER.info("-> With source: " + msg);
    return mongoConfig ->
        mongoConfig.toReadConfig().doWithCollection(coll -> biConsumer.accept(msg, coll));
  }

  private Consumer<MongoConfig> withSink(
      final String msg, final BiConsumer<String, MongoCollection<BsonDocument>> biConsumer) {
    LOGGER.info("-> With sink: " + msg);
    return mongoConfig ->
        mongoConfig.toWriteConfig().doWithCollection(coll -> biConsumer.accept(msg, coll));
  }

  private Consumer<MongoConfig> withMemorySink(
      final String msg, final BiConsumer<String, Dataset<Row>> biConsumer) {
    LOGGER.info("<- With memory sink: " + msg);
    return mongoConfig ->
        biConsumer.accept(msg, getOrCreateSparkSession().sql("SELECT * FROM " + testIdentifier));
  }

  private MongoConfig createMongoConfig() {
    Map<String, String> options = new HashMap<>();
    Arrays.stream(getSparkConf().getAllWithPrefix(MongoConfig.PREFIX))
        .forEach(t -> options.put(MongoConfig.PREFIX + t._1(), t._2()));
    options.put(
        ReadConfig.READ_PREFIX + ReadConfig.COLLECTION_NAME_CONFIG,
        collectionPrefix() + "Source" + testIdentifier);
    return MongoConfig.createConfig(options);
  }

  private List<BsonDocument> createDocuments(final int startInclusive, final int endExclusive) {
    return createDocuments(
        startInclusive, endExclusive, i -> new BsonDocument("_id", idFieldMapper().apply(i)));
  }

  private List<BsonDocument> createDocuments(
      final int startInclusive,
      final int endExclusive,
      final IntFunction<BsonDocument> documentMapper) {
    return IntStream.range(startInclusive, endExclusive)
        .mapToObj(documentMapper)
        .collect(Collectors.toList());
  }

  private IntFunction<BsonString> idFieldMapper() {
    return i -> new BsonString(testIdentifier + "-" + i);
  }
}
