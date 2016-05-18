/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb.spark.api.java;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.JavaTypeInference;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.Assert.assertEquals;

public final class MongoSparkTest extends RequiresMongoDB {

    List<Document> counters = asList(Document.parse("{counter: 0}"), Document.parse("{counter: 1}"), Document.parse("{counter: 2}"));

    @Test
    public void shouldBeCreatableFromTheSparkContext() {
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(counters));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc);

        assertEquals(mongoRDD.count(), 3);

        List<Integer> counters = mongoRDD.map(new Function<Document, Integer>() {
            @Override
            public Integer call(final Document x) throws Exception {
                return x.getInteger("counter");
            }
        }).collect();
        assertEquals(counters, asList(0,1,2));
    }

    @Test
    public void shouldBeCreatableFromTheSparkContextWithAlternativeReadAndWriteConfigs() {
        JavaSparkContext jsc = getJavaSparkContext();
        WriteConfig defaultWriteConfig = WriteConfig.create(jsc);
        ReadConfig defaultReadConfig = ReadConfig.create(jsc);
        Map<String, String> configOverrides = new HashMap<String, String>();
        configOverrides.put("collection", getCollectionName() + "New");
        configOverrides.put("writeConcern.w", "majority");
        configOverrides.put("readPreference.name", "primaryPreferred");
        WriteConfig writeConfig = WriteConfig.create(configOverrides, defaultWriteConfig);
        ReadConfig readConfig = ReadConfig.create(configOverrides, defaultReadConfig);

        MongoSpark.save(jsc.parallelize(counters), writeConfig);
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc, readConfig);

        assertEquals(mongoRDD.count(), 3);
        List<Integer> counters = mongoRDD.map(new Function<Document, Integer>() {
            @Override
            public Integer call(final Document x) throws Exception {
                return x.getInteger("counter");
            }
        }).collect();
        assertEquals(counters, asList(0,1,2));
    }

    @Test
    public void shouldBeAbleToHandleNoneExistentCollections() {
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(getJavaSparkContext());
        assertEquals(mongoRDD.count(), 0);
    }

    @Test
    public void shouldBeAbleToQueryViaAPipeLine() {
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(counters));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc);

        assertEquals(mongoRDD.withPipeline(singletonList(Document.parse("{$match: { counter: {$gt: 0}}}"))).count(), 2);
        assertEquals(mongoRDD.withPipeline(singletonList(BsonDocument.parse("{$match: { counter: {$gt: 0}}}"))).count(), 2);
        assertEquals(mongoRDD.withPipeline(singletonList(Aggregates.match(Filters.gt("counter", 0)))).count(), 2);
    }

    @Test
    public void shouldBeAbleToHandleDifferentCollectionTypes() {
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(asList(BsonDocument.parse("{counter: 0}"), BsonDocument.parse("{counter: 1}"),
                BsonDocument.parse("{counter: 2}"))), BsonDocument.class);
        JavaMongoRDD<BsonDocument> mongoRDD = MongoSpark.load(jsc, BsonDocument.class);

        assertEquals(mongoRDD.count(), 3);
    }

    @Test
    public void shouldBeAbleToCreateADataFrameByInferringTheSchema() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(counters));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc);

        StructField _idField = createStructField("_id", ObjectIdStruct(), true);
        StructField countField = createStructField("counter", DataTypes.IntegerType, true);
        StructType expectedSchema = createStructType(asList(_idField, countField));

        // when
        DataFrame dataFrame = mongoRDD.toDF();

        // then
        assertEquals(dataFrame.schema(), expectedSchema);
        assertEquals(dataFrame.count(), 3);
    }

    @Test
    public void shouldBeAbleToCreateADataFrameUsingJavaBean() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(counters));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc);

        StructType expectedSchema = (StructType) JavaTypeInference.inferDataType(Counter.class)._1();

        // when
        DataFrame dataFrame = mongoRDD.toDF(Counter.class);

        // then
        assertEquals(dataFrame.schema(), expectedSchema);
        assertEquals(dataFrame.count(), 3);
    }

    @Test
    public void shouldBeAbleToCreateADatasetUsingJavaBean() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(counters));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc);

        StructType expectedSchema = (StructType) JavaTypeInference.inferDataType(Counter.class)._1();

        // when
        Dataset<Counter> dataset = mongoRDD.toDS(Counter.class);

        // then
        assertEquals(dataset.schema(), expectedSchema);
        assertEquals(dataset.count(), 3);

        assertEquals(dataset.map(new MapFunction<Counter, Integer>(){
            @Override
            public Integer call(final Counter counter) throws Exception {
                return counter.getCounter();
            }
        }, Encoders.INT()).collectAsList(), asList(0, 1, 2));
    }

    @Test(expected=SparkException.class)
    public void shouldThrowWhenCreatingADatasetWithInvalidData() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(asList(Document.parse("{counter: 'a'}"), Document.parse("{counter: 'b'}"))));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc);

        // when
        Dataset<Counter> dataset = mongoRDD.toDS(Counter.class);

        // then
        List<Integer> test = dataset.map(new MapFunction<Counter, Integer>() {
            @Override
            public Integer call(final Counter counter) throws Exception {
                return counter.getCounter();
            }
        }, Encoders.INT()).collectAsList();
    }

    @Test
    public void useDefaultValuesWhenCreatingADatasetWithMissingData() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(asList(Document.parse("{name: 'a'}"), Document.parse("{name: 'b'}"))));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc);

        // when
        Dataset<Counter> dataset = mongoRDD.toDS(Counter.class);

        Dataset<Integer> test = dataset.map(new MapFunction<Counter, Integer>() {
            @Override
            public Integer call(final Counter counter) throws Exception {
                return counter.getCounter();
            }
        }, Encoders.INT());

        // then - default values
        assertEquals(dataset.map(new MapFunction<Counter, Integer>(){
            @Override
            public Integer call(final Counter counter) throws Exception {
                return counter.getCounter();
            }
        }, Encoders.INT()).collectAsList(), asList(null, null));
    }

}
