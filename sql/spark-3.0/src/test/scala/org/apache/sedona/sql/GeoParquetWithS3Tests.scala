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

import org.scalatest.BeforeAndAfterAll

class GeoParquetWithS3Tests extends TestBaseScala with BeforeAndAfterAll {
  //val location_root = "D:/5_data/census_tiger/newyork"

  describe("GeoParquet IO tests"){

    it("Load Shapefile Save to Parquet file "){
      //val geometryRdd = ShapefileReader.readToPolygonRDD(sparkSession.sparkContext,location_root);
      //val df = Adapter.toDf(geometryRdd,sparkSession);
      //df.write.format("geoparquet").mode(SaveMode.Overwrite).save(location_root + "/output")
    }

    it("Load geoparquet from local file ") {
      //val df = sparkSession.read.format("geoparquet").load(location_root+ "/output/part-00000-3e1739c4-1399-41e0-8f1b-ad5c61b7e669-c000.snappy.parquet")
      //df.show(10)
    }

    it("Load min geoparquet from S3 ") {
      //val df = sparkSession.read.format("geoparquet").load("s3a://bkt-data/part-00000-3e1739c4-1399-41e0-8f1b-ad5c61b7e669-c000.snappy.parquet")
      //val filtereddf = df.where(
      //  "MTFCC =='S1400' AND ST_Intersects(geometry, ST_GeomFromText('POLYGON ((-78.9074 42.9382, -78.9074 42.9012, -78.8432 42.9012, -78.8432 42.9382, -78.9074 42.9382))'))")
     // System.out.println("count is " + filtereddf.count())
      //filtereddf.show(10)
    }

    it("Load max geoparquet from S3 ") {
      var startTime = System.currentTimeMillis();
      //val df = sparkSession.read.format("geoparquet").load("s3a://bkt-data/geoparquets/DLTB20w.parquet")
      val df = sparkSession.read.format("geoparquet").load("D:\\5_data\\DLTB-20w\\output\\DLTB20w.parquet")
      var time1 = System.currentTimeMillis();
      System.out.println("load data costs " + (time1 - startTime) + " ms")
      val filtereddf = df.where(
        "TBBH ==  '129' AND ST_Intersects(geometry, ST_GeomFromText('POLYGON ((115.2220 31.8209, 115.4678 31.8209, 115.4678 31.6145, 115.2220 31.6145, 115.2220 31.8209))'))")
      filtereddf.select("geometry","TBBH");
      var time2 = System.currentTimeMillis();
      System.out.println("count is " + filtereddf.count())
      System.out.println("filter data costs " + (time2 - time1) + " ms")
      filtereddf.show(1)
      var time3 = System.currentTimeMillis();
      System.out.println("show data costs " + (time3 - time2) + " ms")
    }
  }
}
