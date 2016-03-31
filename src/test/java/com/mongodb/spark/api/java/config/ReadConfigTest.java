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

package com.mongodb.spark.api.java.config;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.spark.api.java.RequiresMongoDB;
import com.mongodb.spark.config.ReadConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class ReadConfigTest extends RequiresMongoDB {

    @Test
    public void shouldBeCreatableFromTheSparkConf() {
        ReadConfig readConfig = ReadConfig.create(getSparkConf());
        ReadConfig expectedReadConfig = ReadConfig.create(getDatabaseName(), getCollectionName(), getMongoClientURI(), 1000, 64, "_id",
                15, ReadPreference.primary(), ReadConcern.DEFAULT);

        assertEquals(readConfig, expectedReadConfig);
    }

    @Test
    public void shouldBeCreatableFromAJavaMap() {
        Map<String, String> options = new HashMap<String, String>();
        options.put(ReadConfig.databaseNameProperty(), "db");
        options.put(ReadConfig.collectionNameProperty(), "collection");
        options.put(ReadConfig.sampleSizeProperty(), "500");
        options.put(ReadConfig.maxChunkSizeProperty(), "99");
        options.put(ReadConfig.splitKeyProperty(), "ID");
        options.put(ReadConfig.localThresholdProperty(), "0");
        options.put(ReadConfig.readPreferenceNameProperty(), "secondaryPreferred");
        options.put(ReadConfig.readPreferenceTagSetsProperty(), "[{dc: \"east\", use: \"production\"},{}]");
        options.put(ReadConfig.readConcernLevelProperty(), "majority");

        ReadConfig readConfig = ReadConfig.create(options);
        ReadConfig expectedReadConfig = ReadConfig.create("db", "collection", null, 500, 99, "ID", 0,
                ReadPreference.secondaryPreferred(asList(new TagSet(asList(new Tag("dc", "east"), new Tag("use", "production"))), new TagSet())),
                ReadConcern.MAJORITY);

        assertEquals(readConfig, expectedReadConfig);
    }

    @Test
    public void shouldBeCreatableFromAJavaMapAndUseDefaults() {
        Map<String, String> options = new HashMap<String, String>();
        options.put(ReadConfig.databaseNameProperty(), "db");
        options.put(ReadConfig.collectionNameProperty(), "collection");
        options.put(ReadConfig.readPreferenceNameProperty(), "secondaryPreferred");
        options.put(ReadConfig.readConcernLevelProperty(), "majority");

        ReadConfig readConfig = ReadConfig.create(options, ReadConfig.create(getSparkConf()));
        ReadConfig expectedReadConfig = ReadConfig.create("db", "collection", getMongoClientURI(), 1000, 64, "_id", 15,
                ReadPreference.secondaryPreferred(), ReadConcern.MAJORITY);

        assertEquals(readConfig, expectedReadConfig);
    }
}
