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

package com.mongodb.spark.connection

import org.scalatest.{FlatSpec, Matchers}

class MongoClientRefCounterSpec extends FlatSpec with Matchers {

  "MongoClientRefCounter" should "count references as expected" in {
    val counter = new MongoClientRefCounter()

    counter.canAcquire() should equal(false)
    counter.acquire()
    counter.canAcquire() should equal(true)

    counter.release() should equal(1)
    counter.release() should equal(0)

    counter.canAcquire() should equal(false)
  }

  it should "be able to acquire multiple times" in {
    val counter = new MongoClientRefCounter()
    counter.acquire()
    counter.acquire()
    counter.release(2) should equal(0)
  }

  it should "throw an exception for invalid releases of a MongoClient" in {
    val counter = new MongoClientRefCounter()
    an[IllegalStateException] should be thrownBy counter.release()

    counter.acquire()
    an[IllegalStateException] should be thrownBy counter.release(2)
  }
}
