//scalastyle:off
/*
 * Copyright 2016 MongoDB, Inc.
 * Copyright 2014-2015, DataStax, Inc.
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
//scalastyle:on
package com.mongodb.spark.connection

import java.util.concurrent.atomic.AtomicInteger

/**
 * Atomically counts references to MongoClients
 */
private class MongoClientRefCounter {

  private val mongoClientCounts = new AtomicInteger(0)

  /**
   * Indicates if the `MongoClient` can be acquired.
   *
   * Atomically increases reference count only if the reference counter is already greater than 0.
   *
   * @return true if the `MongoClient` can be acquired
   */
  final def canAcquire(): Boolean = {
    mongoClientCounts.get() match {
      case count if count > 0 =>
        mongoClientCounts.getAndIncrement()
        true
      case _ => false
    }
  }

  /**
   * Acquires the `MongoClient`
   *
   * Atomically sets the counter to one
   */
  final def acquire(): Unit = mongoClientCounts.incrementAndGet()

  /**
   * Release the `MongoClient`
   *
   * Atomically decreases reference count by `n`.
   *
   * @throws IllegalStateException if the reference count before decrease is less than `n`
   * @return the MongoClient reference count
   */
  final def release(n: Int = 1): Int = {
    require(n > 0, s"Invalid release amount $n, must be greater than 0")
    val remaining = mongoClientCounts.addAndGet(-n)
    if (remaining < 0) throw new IllegalStateException("Cannot release the MongoClient when it hasn't been acquired")
    remaining
  }
}
