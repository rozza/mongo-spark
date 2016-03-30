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

import com.mongodb._
import com.mongodb.spark.MongoClientFactory
import com.mongodb.spark.config.{MongoSharedConfig, ReadConfig}

import scala.collection.JavaConverters._
import scala.util.Try

private[spark] object DefaultMongoClientFactory {
  def apply(options: collection.Map[String, String]): DefaultMongoClientFactory = {
    require(options.contains(MongoSharedConfig.mongoURIProperty), s"Missing '${MongoSharedConfig.mongoURIProperty}' property from options")
    DefaultMongoClientFactory(options.get(MongoSharedConfig.mongoURIProperty).get, options.get(ReadConfig.localThresholdProperty).map(_.toInt))
  }
}

private[spark] case class DefaultMongoClientFactory(connectionString: String, localThreshold: Option[Int] = None) extends MongoClientFactory {
  require(Try(mongoClientURI).isSuccess, s"Invalid '${MongoSharedConfig.mongoURIProperty}' '$connectionString'")

  def mongoClientURI: MongoClientURI = new MongoClientURI(connectionString)

  override def create(): MongoClient = {
    val clientURI = mongoClientURI
    val hosts = clientURI.getHosts.asScala.map(new ServerAddress(_))
    val credentials = Option(clientURI.getCredentials) match {
      case Some(credential) => List(credential)
      case None             => List.empty[MongoCredential]
    }
    val clientOptions = localThreshold match {
      case Some(lt) => MongoClientOptions.builder(clientURI.getOptions).localThreshold(lt).build()
      case None     => clientURI.getOptions
    }
    new MongoClient(hosts.asJava, credentials.asJava, clientOptions)
  }
}
