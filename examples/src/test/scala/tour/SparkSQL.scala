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

package tour

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.bson.Document
import com.mongodb.spark.sql.helpers.UDF


/**
 * The spark SQL code example see docs/1-sparkSQL.md
 */
object SparkSQL extends TourHelper {

  //scalastyle:off method.length
  /**
   * Run this main method to see the output of this quick example or copy the code into the spark shell
   *
   * @param args takes an optional single argument for the connection string
   * @throws Throwable if an operation fails
   */
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args) // Don't copy and paste as its already configured in the shell

    // Load sample data
    import com.mongodb.spark._
    val docs =
      """
        |{"_id": {$oid: "5cfa614103b1094078010fa7"}, "name": "Bilbo Baggins", "age": 50}
        |{"name": "Gandalf", "age": 1000}
        |{"name": "Thorin", "age": 195}
        |{"name": "Balin", "age": 178}
        |{"name": "Kíli", "age": 77}
        |{"name": "Dwalin", "age": 169}
        |{"name": "Óin", "age": 167}
        |{"name": "Glóin", "age": 158}
        |{"name": "Fíli", "age": 82}
        |{"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
    MongoSpark.save(sc.parallelize(docs.map(Document.parse)))

    // Create SparkSession
    val sparkSession = SparkSession.builder().getOrCreate()

    // Import the SQL helper
    val df = MongoSpark.load(sparkSession)
    df.printSchema()


    // Characters younger than 100
    val dff = df.filter(df("_id") === UDF.ObjectId("5cfa614103b1094078010fa7"))
    dff.show()
    dff.explain()
  }
}
