package org.apache.sedona.spark

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialOperator.{JoinQuery, SpatialPredicate}
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.serializer.KryoSerializer
import org.locationtech.jts.geom.Geometry

import java.util
import java.util.List

object  JoinQueryMain {
  def main(args: Array[String]): Unit = {
    var queryLocation: String = "/D:/5_data/japanese/ribenhongshui1"
    var windowsLocation: String = "/D:/5_data/japanese/landuse"
    if(args.length >= 2){
      queryLocation = args.apply(0)
      windowsLocation = args.apply(1)
    }
    System.out.println("queryLocation is " + queryLocation  + ", windowsLocation is " + windowsLocation)

    val sc:SparkContext = initializeSparkContext()
    val spatialRDD: SpatialRDD[Geometry] = ShapefileReader.readToGeometryRDD(sc, queryLocation)
    spatialRDD.analyze()
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 100)
    spatialRDD.buildIndex(IndexType.RTREE,true)
    System.out.println("queryrdd analyze and Partition over ")

    val windowsRdd: SpatialRDD[Geometry] = ShapefileReader.readToGeometryRDD(sc, windowsLocation)
    windowsRdd.spatialPartitioning(spatialRDD.getPartitioner)
    System.out.println("windowsRdd Partition over ")

    val actualResultRdd: JavaPairRDD[Geometry, util.List[Geometry]] = JoinQuery.SpatialJoinQuery(spatialRDD, windowsRdd, true, SpatialPredicate.INTERSECTS)

    val count = actualResultRdd.count
    System.out.println("actualResult count is " + count);
  }

  def initializeSparkContext(): SparkContext ={
    val conf: SparkConf = new SparkConf().setAppName("sedona join query");
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
    conf.set("spark.kryoserializer.buffer.max", "1024m")
    conf.set("spark.executor.memory","4g")
    val sc: SparkContext = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    sc
  }

}
