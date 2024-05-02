package org.apache.sedona.core.formatMapper.shapefileParser.shapes;

import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class BigShapefileReaderTest extends TestBase {
    @BeforeClass
    public static void onceExecutedBeforeAll()
            throws IOException
    {
        initialize(ShapefileReaderTest.class.getName());
    }

    @Test
    public void testReadAndToDataFrame(){

        String inputLocation = "/D:/1_software/gis/geoserver/geoserver-2.21.0-bin/data_dir/data/nyc/japanese";
        SpatialRDD shapeRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);
        JavaRDD<Geometry>  geometryRdd = shapeRDD.rawSpatialRDD ;
        geometryRdd.map((geo) -> {
            System.out.println("srid" + geo.getSRID()) ;
            return geo.getUserData() ;
        }).collect();
        //assertEquals(0, shapeRDD.getRawSpatialRDD().count());
    }
}
