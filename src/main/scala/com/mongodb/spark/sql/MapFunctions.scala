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

package com.mongodb.spark.sql

import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, NullType, StringType, TimestampType, _}

import org.bson._
import com.mongodb.spark.sql.types.BsonCompatibility

private[spark] object MapFunctions {

  // scalastyle:off cyclomatic.complexity null
  def documentToRow(bsonDocument: BsonDocument, schema: StructType, requiredColumns: Array[String] = Array.empty[String]): Row = {
    val values: Array[(Any, StructField)] = schema.fields.map(field =>
      bsonDocument.containsKey(field.name) match {
        case true  => (convertValue(bsonDocument.get(field.name), field.dataType), field)
        case false => (null, field)
      })

    val requiredValues = requiredColumns.nonEmpty match {
      case true =>
        val requiredValueMap = Map(values.collect({
          case (rowValue, rowField) if requiredColumns.contains(rowField.name) =>
            (rowField.name, (rowValue, rowField))
        }): _*)
        requiredColumns.collect({ case name => requiredValueMap.getOrElse(name, null) })
      case false => values
    }
    new GenericRowWithSchema(requiredValues.map(_._1), DataTypes.createStructType(requiredValues.map(_._2)))
  }

  def rowToDocument(row: Row): BsonDocument = {
    val document = new BsonDocument()
    row.schema.fields.zipWithIndex.foreach({
      case (field, i) if row.isNullAt(i) => document.append(field.name, new BsonNull())
      case (field, i)                    => document.append(field.name, elementTypeToBsonValue(field.dataType, row.get(i)))
    })
    document
  }

  private def convertValue(data: BsonValue, elementType: DataType): Any = {
    Option(data) match {
      case Some(element) => Try(castToDataType(element, elementType)) match {
        case Success(value) => value
        case Failure(ex)    => throw new RuntimeException(s"Could not convert $element to ${elementType.typeName}")
      }
      case None => data
    }
  }

  private def castToDataType(element: BsonValue, elementType: DataType): Any = {
    element.getBsonType match {
      case BsonType.NULL => null
      case _ => elementType match {
        case _: BinaryType        => element.asBinary().getData
        case _: BooleanType       => element.asBoolean().getValue
        case _: DateType          => new Date(element.asDateTime().getValue)
        case _: DoubleType        => element.asNumber().doubleValue()
        case _: IntegerType       => element.asNumber().intValue()
        case _: LongType          => element.asNumber().longValue()
        case _: NullType          => null
        case _: StringType        => bsonValueToString(element)
        case _: TimestampType     => new Timestamp(element.asDateTime().getValue)
        case arrayType: ArrayType => element.asArray().getValues.asScala.map(castToDataType(_, arrayType.elementType))
        case schema: StructType   => castToStructType(schema, element)
        case _                    => throw new UnsupportedOperationException(s"$elementType is currently unsupported with value: $element")
      }
    }
  }

  private def bsonValueToString(element: BsonValue): String = {
    element.getBsonType match {
      case BsonType.STRING    => element.asString().getValue
      case BsonType.OBJECT_ID => element.asObjectId().getValue.toHexString
      case BsonType.ARRAY     => element.asArray().asScala.mkString("[", ",", "]")
      case _                  => new BsonDocument("f", element).toJson.stripPrefix("""{ "f" :""").stripSuffix("}").trim()
    }
  }

  private def castToStructType(structType: StructType, element: BsonValue): Any = {
    structType match {
      case BsonCompatibility.ObjectId()            => BsonCompatibility.ObjectId(element.asObjectId(), structType)
      case BsonCompatibility.MinKey()              => BsonCompatibility.MinKey(element.asInstanceOf[BsonMinKey], structType)
      case BsonCompatibility.MaxKey()              => BsonCompatibility.MaxKey(element.asInstanceOf[BsonMaxKey], structType)
      case BsonCompatibility.Timestamp()           => BsonCompatibility.Timestamp(element.asInstanceOf[BsonTimestamp], structType)
      case BsonCompatibility.JavaScript()          => BsonCompatibility.JavaScript(element.asInstanceOf[BsonJavaScript], structType)
      case BsonCompatibility.JavaScriptWithScope() => BsonCompatibility.JavaScriptWithScope(element.asInstanceOf[BsonJavaScriptWithScope], structType)
      case BsonCompatibility.RegularExpression()   => BsonCompatibility.RegularExpression(element.asInstanceOf[BsonRegularExpression], structType)
      case BsonCompatibility.Undefined()           => BsonCompatibility.Undefined(element.asInstanceOf[BsonUndefined], structType)
      case BsonCompatibility.Binary()              => BsonCompatibility.Binary(element.asInstanceOf[BsonBinary], structType)
      case BsonCompatibility.Symbol()              => BsonCompatibility.Symbol(element.asInstanceOf[BsonSymbol], structType)
      case BsonCompatibility.DbPointer()           => BsonCompatibility.DbPointer(element.asInstanceOf[BsonDbPointer], structType)
      case _                                       => documentToRow(element.asInstanceOf[BsonDocument], structType)
    }
  }

  private def castFromStructType(structType: StructType, element: Row): BsonValue = {
    structType match {
      case BsonCompatibility.ObjectId()            => BsonCompatibility.ObjectId(element)
      case BsonCompatibility.MinKey()              => BsonCompatibility.MinKey(element)
      case BsonCompatibility.MaxKey()              => BsonCompatibility.MaxKey(element)
      case BsonCompatibility.Timestamp()           => BsonCompatibility.Timestamp(element)
      case BsonCompatibility.JavaScript()          => BsonCompatibility.JavaScript(element)
      case BsonCompatibility.JavaScriptWithScope() => BsonCompatibility.JavaScriptWithScope(element)
      case BsonCompatibility.RegularExpression()   => BsonCompatibility.RegularExpression(element)
      case BsonCompatibility.Undefined()           => BsonCompatibility.Undefined(element)
      case BsonCompatibility.Binary()              => BsonCompatibility.Binary(element)
      case BsonCompatibility.Symbol()              => BsonCompatibility.Symbol(element)
      case BsonCompatibility.DbPointer()           => BsonCompatibility.DbPointer(element)
      case _                                       => rowToDocument(element)
    }
  }

  private def elementTypeToBsonValue(elementType: DataType, element: Any): BsonValue = {
    elementType match {
      case _: BinaryType        => new BsonBinary(element.asInstanceOf[Array[Byte]])
      case _: BooleanType       => new BsonBoolean(element.asInstanceOf[Boolean])
      case _: DateType          => new BsonDateTime(element.asInstanceOf[Date].getTime)
      case _: DoubleType        => new BsonDouble(element.asInstanceOf[Double])
      case _: IntegerType       => new BsonInt32(element.asInstanceOf[Int])
      case _: LongType          => new BsonInt64(element.asInstanceOf[Long])
      case _: StringType        => new BsonString(element.asInstanceOf[String])
      case _: TimestampType     => new BsonDateTime(element.asInstanceOf[Timestamp].getTime)
      case arrayType: ArrayType => arrayTypeToBsonValue(arrayType.elementType, element.asInstanceOf[Seq[_]])
      case schema: StructType   => castFromStructType(schema, element.asInstanceOf[Row])
      case _ =>
        throw new UnsupportedOperationException(s"$elementType is currently unsupported with value: $element")
    }
  }

  private def arrayTypeToBsonValue(elementType: DataType, data: Seq[Any]): BsonValue = {
    val internalData = elementType match {
      case subDocuments: StructType => data.map(x => rowToDocument(x.asInstanceOf[Row])).asJava
      case subArray: ArrayType      => data.map(x => arrayTypeToBsonValue(subArray.elementType, x.asInstanceOf[Seq[Any]])).asJava
      case _                        => data.map(x => elementTypeToBsonValue(elementType, x)).asJava
    }
    new BsonArray(internalData)
  }
  // scalastyle:on cyclomatic.complexity null
}
