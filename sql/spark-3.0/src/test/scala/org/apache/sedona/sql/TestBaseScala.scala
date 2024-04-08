/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.sql

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.spark.SedonaContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait TestBaseScala extends FunSpec with BeforeAndAfterAll {
  Logger.getRootLogger().setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org.apache.sedona.core").setLevel(Level.WARN)

  val s3accessKeyAws = "minioadmin"
  val s3secretKeyAws = "minioadmin"
  val connectionTimeOut = "600000"
  val s3endPointLoc: String = "http://127.0.0.1:9000"

  val warehouseLocation = System.getProperty("user.dir") + "/target/"

  val sparkSession = SedonaContext.builder().
    master("local[*]").appName("sedonasqlScalaTest")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    // We need to be explicit about broadcasting in tests.
    .config("sedona.join.autoBroadcastJoinThreshold", "-1")
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
    .config("spark.hadoop.fs.s3a.endpoint", s3endPointLoc)
    .config("spark.hadoop.fs.s3a.access.key", s3accessKeyAws)
    .config("spark.hadoop.fs.s3a.secret.key", s3secretKeyAws)
    .config("spark.hadoop.fs.s3a.connection.timeout", connectionTimeOut)

    .config("spark.sql.debug.maxToStringFields", "100")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()

  val resourceFolder = System.getProperty("user.dir") + "../../../core/src/test/resources/"

  override def beforeAll(): Unit = {
    SedonaContext.create(sparkSession)
  }

  override def afterAll(): Unit = {
    //SedonaSQLRegistrator.dropAll(spark)
    //spark.stop
  }

  def loadCsv(path: String): DataFrame = {
    sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(path)
  }
}
