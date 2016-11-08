package org.safegraph;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class PointByPolygonJudgement implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Point>>>, Point, Polygon>, Serializable {

    @Override
    public Iterator<Tuple2<Point, Polygon>> call(Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Point>>> cogroup) throws Exception {
        HashSet<Tuple2<Point, Polygon>> result = new HashSet<>();

        Tuple2<Iterable<STRtree>, Iterable<Point>> cogroupTupleList = cogroup._2;
        for (Point point : cogroupTupleList._2) {
            for (STRtree tree : cogroupTupleList._1) {
                List<Polygon> polygons = tree.query(point.getEnvelopeInternal());
                for (Polygon polygon : polygons) {
                    if (polygon.contains(point)) {
                        result.add(new Tuple2(point, polygon));
                    }
                }
            }
        }

        return result.iterator();
    }
}
