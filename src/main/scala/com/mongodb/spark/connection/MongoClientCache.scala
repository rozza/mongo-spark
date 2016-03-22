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

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import com.mongodb.MongoClient
import com.mongodb.spark.{Logging, MongoClientFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
 * A lockless cache for a MongoClient.
 *
 * Allows multiple users access to a MongoClient. Closes the `MongoClient` when they're are no longer used.
 *
 * @param keepAlive the duration to keep alive a MongoClient so that it can be reused by another consumer
 */
private[spark] final class MongoClientCache(keepAlive: Duration) extends Logging {

  private val refCounter = new MongoClientRefCounter
  private val cache = new AtomicReference[MongoClient]()
  private val deferredReleases = new AtomicReference[ReleaseTask]

  @tailrec
  def acquire(mongoClientFactory: MongoClientFactory): MongoClient = {
    Option(cache.get) match {
      case Some(mongoClient) =>
        refCounter.canAcquire() match {
          case true  => mongoClient
          case false => acquire(mongoClientFactory)
        }
      case None =>
        val createdMongoClient = mongoClientFactory.create()
        logClient(createdMongoClient)
        cache.compareAndSet(null, createdMongoClient) match { // scalastyle:ignore
          case true =>
            refCounter.acquire()
            createdMongoClient
          case false =>
            logClient(createdMongoClient, closing = true)
            createdMongoClient.close()
            acquire(mongoClientFactory)
        }
    }
  }

  /**
   * Releases previously acquired mongoClient. Once the mongoClient is released by all threads and
   * the `releaseDelayMillis` timeout passes, the mongoClient is destroyed by calling `destroy` function and
   * removed from the cache.
   */
  def release(releaseDelay: Duration = keepAlive) {
    Option(cache.get) match {
      case Some(client) =>
        if (releaseDelay.toMillis == 0 || scheduledExecutorService.isShutdown) {
          releaseImmediately()
        } else {
          releaseDeferred(releaseDelay, 1)
        }
      case None =>
    }
  }

  /**
   * Shuts down the background deferred `release` scheduler and forces all pending release tasks to be executed
   */
  def shutdown() {
    scheduledExecutorService.shutdown()
    Option(deferredReleases.getAndSet(null)) match { // scalastyle:ignore
      case Some(releaseTask) =>
        releaseTask.run()
      case None =>
    }
  }

  private def releaseImmediately(count: Int = 1): Unit = {
    Try(refCounter.release(count)) match {
      case Success(0) =>
        Option(cache.getAndSet(null)) match { // scalastyle:ignore
          case Some(mongoClient) =>
            logClient(mongoClient, closing = true)
            mongoClient.close()
          case None =>
        }
      case Failure(e) => logWarning(e.getMessage)
      case _          =>
    }
  }

  private def releaseDeferred(releaseDelay: Duration, count: Int): Unit = {
    val newTime = System.nanoTime() + releaseDelay.toNanos
    Option(deferredReleases.get) match {
      case Some(oldTask) => deferredReleases.compareAndSet(oldTask, ReleaseTask(oldTask.count + count, math.max(oldTask.scheduledTime, newTime)))
      case None          => deferredReleases.compareAndSet(null, ReleaseTask(count, newTime)) // scalastyle:ignore
    }
  }

  private val processPendingReleasesTask = new Runnable() {
    override def run() {
      val now = System.nanoTime()
      Option(deferredReleases.getAndSet(null)) match { // scalastyle:ignore
        case Some(releaseTask) if releaseTask.scheduledTime <= now => releaseTask.run()
        case Some(releaseTask) => deferredReleases.compareAndSet(null, releaseTask) // scalastyle:ignore
        case _ =>
      }
    }
  }

  private val scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
    override def newThread(r: Runnable) = {
      val thread = Executors.defaultThreadFactory().newThread(r)
      thread.setDaemon(true)
      thread
    }
  })

  // This must be high enough so it doesn't cause too much CPU usage,
  // but also low enough to allow for acceptable releaseDelayMillis resolution.
  private val period = 100
  scheduledExecutorService.scheduleAtFixedRate(processPendingReleasesTask, period, period, TimeUnit.MILLISECONDS)

  private case class ReleaseTask(count: Int, scheduledTime: Long) extends Runnable {
    override def run() {
      releaseImmediately(count)
    }
  }

  private def logClient(mongoClient: MongoClient, closing: Boolean = false): Unit = {
    val status = if (closing) "Closing" else "Creating"
    logInfo(s"""$status MongoClient: ${mongoClient.getServerAddressList.asScala.map(_.toString).mkString("[", ",", "]")}""")
  }

}
