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

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
class ShapeDataToParquetTests extends TestBaseScala with BeforeAndAfterAll{
  val jap_parquet_path = "D:/1_software/gis/geoserver/geoserver-2.21.0-bin/data_dir/data/nyc/japanese1"
   def beforeAll1111(): Unit = {
    super.beforeAll()
    val inputLocation = "/D:/1_software/gis/geoserver/geoserver-2.21.0-bin/data_dir/data/nyc/japanese"
    val shapeRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, inputLocation)
    val fieldNames  = "name,geom"
    val fields = fieldNames.split(",").map(field => {
      if("name".equals(field)){
        StructField(field,StringType,nullable = true)
      }else{
        StructField(field,GeometryUDT,nullable = true)
      }
    })
    val geometryRdd: RDD[Row]= shapeRDD.rawSpatialRDD
    .map(geo => Row("TEST NAME",geo))
    val schema: StructType = StructType(fields)
    val df = sparkSession.createDataFrame(geometryRdd,schema)
    df.createOrReplaceTempView("jp")
    //df.where("name == 'TEST NAME' ").show(10000)
    df.write.format("geoparquet").save(jap_parquet_path);
    System.out.println("save done!")
  }

  describe("select * "){
    it("Push down Or(filters...)") {
      val gdf = sparkSession.read.format("geoparquet").load(jap_parquet_path)
      gdf.show(100)
    }
  }
}
