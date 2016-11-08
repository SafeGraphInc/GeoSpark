package org.safegraph;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.spark.api.java.JavaPairRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import scala.Tuple2;

public class PointToPolygonJoinQuery {

    public JavaPairRDD<Point, Polygon> join(PointRDD pointRDD, PolygonRDD polygonRDD) {
        if (polygonRDD.indexedRDD == null) {
            throw new IllegalArgumentException("IndexedRDD is null, please call buildIndex");
        }

        JavaPairRDD<Integer, Tuple2<Iterable<STRtree>, Iterable<Point>>> cogroupResult = polygonRDD.indexedRDD.cogroup(pointRDD.gridPointRDD);
        return cogroupResult.flatMapToPair(new PointByPolygonJudgement());
    }
}
