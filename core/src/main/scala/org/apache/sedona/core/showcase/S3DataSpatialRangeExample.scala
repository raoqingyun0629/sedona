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

package org.apache.sedona.core.showcase

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialOperator.{JoinQuery, SpatialPredicate}
import org.apache.sedona.core.spatialRDD.{PointRDD, PolygonRDD}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory}


/**
  * The Class ScalaExample.
 *  使用Minio中的S3协议数据，进行分布式空间分析。
 *  借鉴：https://github.com/nitisht/cookbook/blob/master/docs/apache-spark-with-minio.md
  */
object S3DataSpatialRangeExample extends App {

  val conf = new SparkConf().setAppName("SedonaRunnableExample").setMaster("local[2]")
  val s3accessKeyAws = "minioadmin"
  val s3secretKeyAws = "minioadmin"
  val connectionTimeOut = "600000"
  val s3endPointLoc: String = "http://127.0.0.1:9000"
  conf.set("spark.serializer", classOf[KryoSerializer].getName)
  conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
  conf.set("spark.hadoop.fs.s3a.endpoint", s3endPointLoc)
  conf.set("spark.hadoop.fs.s3a.access.key", s3accessKeyAws)
  conf.set("spark.hadoop.fs.s3a.secret.key", s3secretKeyAws)
  conf.set("spark.hadoop.fs.s3a.connection.timeout", connectionTimeOut)

  conf.set("spark.sql.debug.maxToStringFields", "100")
  conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
  conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  //conf.set("fs.s3a.connection.ssl.enabled", "true")

  val sc = new SparkContext(conf)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val PointRDDInputLocation = "s3a://bkt-data/arealm-small.csv"
  val PointRDDSplitter = FileDataSplitter.CSV
  val PointRDDIndexType = IndexType.RTREE
  val PointRDDNumPartitions = 5
  val PointRDDOffset = 1

  val PolygonRDDInputLocation = "s3a://bkt-data/primaryroads-polygon.csv"
  val PolygonRDDSplitter = FileDataSplitter.CSV
  val PolygonRDDNumPartitions = 5
  val PolygonRDDStartOffset = 0
  val PolygonRDDEndOffset = 9

  val geometryFactory = new GeometryFactory()
  val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
  val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
  val joinQueryPartitioningType = GridType.QUADTREE
  val eachQueryLoopTimes = 1

  //testSpatialJoinQueryUsingIndex()
  loadGeoPaquartFromS3()
  sc.stop()
  System.out.println("All DEMOs passed!")



  /**
    * Test spatial join query using index.
    *
    * @throws Exception the exception
    */
  def testSpatialJoinQueryUsingIndex() {
    val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)

    objectRDD.spatialPartitioning(joinQueryPartitioningType)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
    System.out.println("sss111")
    objectRDD.buildIndex(PointRDDIndexType, true)

    objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, true, SpatialPredicate.COVERED_BY).count()
      System.out.println("resultSize is " + resultSize)
    }
    System.out.println("sss222")
  }

  def loadGeoPaquartFromS3(): Unit = {
    val sqlContext = new SQLContext(sc)
    val sparkSession = sqlContext.sparkSession
    val df = sparkSession.read.format("geoparquet")
      .load("s3a://bkt-data/part-00000-3e1739c4-1399-41e0-8f1b-ad5c61b7e669-c000.snappy.parquet")
    df.show(10)
  }

}
