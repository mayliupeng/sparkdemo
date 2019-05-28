package kfk.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class VarJava {
    public static JavaSparkContext getsc() {
        SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        return sc;
    }

    public static void main(String[] args) {
        List list = Arrays.asList(1,2,3,4);
        JavaSparkContext sc = getsc();

        final Broadcast broadcast = sc.broadcast(10);

        JavaRDD rdd = sc.parallelize(list);

        JavaRDD values = rdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer value) throws Exception {
                return value * Integer.parseInt(String.valueOf(broadcast.getValue()));
            }
        });

        values.foreach(new VoidFunction() {
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });
    }
}
