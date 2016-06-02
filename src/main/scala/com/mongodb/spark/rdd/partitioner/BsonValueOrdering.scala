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

package com.mongodb.spark.rdd.partitioner

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import org.bson._

trait BsonValueOrdering extends Ordering[BsonValue] {

  // scalastyle:off cyclomatic.complexity
  private val bsonTypeComparisonMap: Map[BsonType, Int] = Map(
    BsonType.MIN_KEY -> 1,
    BsonType.NULL -> 2,
    BsonType.INT32 -> 3,
    BsonType.INT64 -> 3,
    BsonType.DOUBLE -> 3,
    BsonType.SYMBOL -> 4,
    BsonType.STRING -> 4,
    BsonType.DOCUMENT -> 5,
    BsonType.ARRAY -> 6,
    BsonType.BINARY -> 7,
    BsonType.OBJECT_ID -> 8,
    BsonType.BOOLEAN -> 9,
    BsonType.DATE_TIME -> 10,
    BsonType.TIMESTAMP -> 11,
    BsonType.REGULAR_EXPRESSION -> 12,
    BsonType.MAX_KEY -> 13
  )

  /**
   *
   * Returns an integer whose sign communicates how x compares to y.
   *
   * The result sign has the following meaning:
   *
   * - negative if x < y
   * - positive if x > y
   * - zero otherwise (if x == y)
   *
   * When comparing values of different BSON types, MongoDB uses the following comparison order, from lowest to highest:
   *
   * 1. MinKey (internal type)
   * 2. Null
   * 3. Numbers (ints, longs, doubles)
   * 4. Symbol, String
   * 5. Object
   * 6. Array
   * 7. BinData
   * 8. ObjectId
   * 9. Boolean
   * 10. Date
   * 11. Timestamp
   * 12. Regular Expression
   * 13. MaxKey (internal type)
   */
  override def compare(x: BsonValue, y: BsonValue): Int = {
    (x.getBsonType, y.getBsonType) match {
      case (BsonType.MIN_KEY, BsonType.MIN_KEY)     => 0
      case (BsonType.NULL, BsonType.NULL)           => 0
      case (isBsonNumber(), isBsonNumber())         => x.asNumber.doubleValue.compareTo(y.asNumber.doubleValue)
      case (BsonType.STRING, BsonType.STRING)       => x.asString.getValue.compareTo(y.asString.getValue)
      case (BsonType.SYMBOL, BsonType.STRING)       => x.asSymbol.getSymbol.compareTo(y.asString.getValue)
      case (BsonType.STRING, BsonType.SYMBOL)       => x.asString.getValue.compareTo(y.asSymbol.getSymbol)
      case (BsonType.SYMBOL, BsonType.SYMBOL)       => x.asSymbol.getSymbol.compareTo(y.asSymbol.getSymbol)
      case (BsonType.DOCUMENT, BsonType.DOCUMENT)   => compareDocuments(x.asDocument, y.asDocument)
      case (BsonType.ARRAY, BsonType.ARRAY)         => compareArrays(x.asArray, y.asArray)
      case (BsonType.BINARY, BsonType.BINARY)       => compareBinary(x.asBinary, y.asBinary)
      case (BsonType.OBJECT_ID, BsonType.OBJECT_ID) => x.asObjectId.getValue.compareTo(y.asObjectId.getValue)
      case (BsonType.BOOLEAN, BsonType.BOOLEAN)     => x.asBoolean.getValue.compareTo(y.asBoolean().getValue)
      case (BsonType.DATE_TIME, BsonType.DATE_TIME) => x.asDateTime.getValue.compareTo(y.asDateTime().getValue)
      case (BsonType.TIMESTAMP, BsonType.TIMESTAMP) => compareTimestamps(x.asTimestamp, y.asTimestamp)
      case (BsonType.REGULAR_EXPRESSION, BsonType.REGULAR_EXPRESSION) =>
        compareRegularExpressions(x.asRegularExpression, y.asRegularExpression)
      case (xType, yType) =>
        val xSortScore = bsonTypeComparisonMap.getOrElse(x.getBsonType, 14) // scalastyle:ignore
        val ySortScore = bsonTypeComparisonMap.getOrElse(y.getBsonType, 14) // scalastyle:ignore
        xSortScore.compareTo(ySortScore)
    }
  }

  private def compareDocuments(x: BsonDocument, y: BsonDocument): Int = {
    val xKeyValues: Seq[(String, BsonValue)] = x.entrySet().asScala.map(x => (x.getKey, x.getValue)).toSeq
    val yKeyValues: Seq[(String, BsonValue)] = y.entrySet().asScala.map(y => (y.getKey, y.getValue)).toSeq
    compareKeyValues(xKeyValues zip yKeyValues) match {
      case 0 => xKeyValues.length.compareTo(yKeyValues.length)
      case v => v
    }
  }

  private def compareArrays(x: BsonArray, y: BsonArray): Int = {
    compareBsonValues(x.getValues.asScala zip y.getValues.asScala) match {
      case 0 => x.getValues.size.compareTo(y.getValues.size)
      case v => v
    }
  }

  private def compareRegularExpressions(x: BsonRegularExpression, y: BsonRegularExpression): Int = {
    x.getPattern.compareTo(y.getPattern) match {
      case 0 => x.getOptions.compareTo(y.getOptions)
      case v => v
    }
  }

  private def compareBinary(x: BsonBinary, y: BsonBinary): Int = {
    x.getData.length.compareTo(y.getData.length) match {
      case 0 =>
        x.getType.compareTo(y.getType) match {
          case 0 => compareBytes(x.getData zip y.getData)
          case v => v
        }
      case v => v
    }
  }

  private def compareTimestamps(x: BsonTimestamp, y: BsonTimestamp): Int = {
    x.getTime.compareTo(y.getTime) match {
      case 0 => x.getInc.compareTo(y.getInc)
      case v => v
    }
  }

  @tailrec
  private def compareKeyValues(kvs: Seq[((String, BsonValue), (String, BsonValue))]): Int = {
    kvs.headOption match {
      case Some(kv) => compareKeyValues(kv._1, kv._2) match {
        case 0 => compareKeyValues(kvs.tail)
        case v => v
      }
      case None => 0
    }
  }

  private def compareKeyValues(x: (String, BsonValue), y: (String, BsonValue)): Int = {
    x._1.compareTo(y._1) match {
      case 0 => compare(x._2, y._2)
      case v => v
    }
  }

  @tailrec
  private def compareBsonValues(values: Seq[(BsonValue, BsonValue)]): Int = {
    values.headOption match {
      case Some(value) => compare(value._1, value._2) match {
        case 0 => compareBsonValues(values.tail)
        case v => v
      }
      case None => 0
    }
  }

  @tailrec
  private def compareBytes(values: Seq[(Byte, Byte)]): Int = {
    values.headOption match {
      case Some(value) => value._1.compareTo(value._2) match {
        case 0 => compareBytes(values.tail)
        case v => v
      }
      case None => 0
    }
  }

  private object isBsonNumber {
    val bsonNumberTypes = Set(BsonType.INT32, BsonType.INT64, BsonType.DOUBLE)

    def unapply(x: BsonType): Boolean = bsonNumberTypes.contains(x)
  }

  private object isString {
    val bsonNumberTypes = Set(BsonType.SYMBOL, BsonType.STRING)

    def unapply(x: BsonType): Boolean = bsonNumberTypes.contains(x)
  }

}

// scalastyle:on cyclomatic.complexity
