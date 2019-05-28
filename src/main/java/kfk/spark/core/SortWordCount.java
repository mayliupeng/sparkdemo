package kfk.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SortWordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("wordCountApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD lines = sc.textFile("hdfs://qiniuhadoop:9000/user/kafka/datas/wordcount.txt");
        JavaRDD words  =  lines.flatMap(new FlatMapFunction<String,String>() {
            public Iterator call(String line) throws Exception {
                return Arrays.asList(line.split("\t")).iterator();
            }
        });

        JavaPairRDD word = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2 call(String word) throws Exception {
                return new Tuple2(word, 1);
            }
        });

        JavaPairRDD wordcount = word.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        word.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> o) throws Exception {
                System.out.println(o._1 + " : " + o._2);
            }
        });

        JavaPairRDD beingSort = wordcount.mapToPair(new PairFunction<Tuple2, Integer, String>() {
            public Tuple2 call(Tuple2 o) throws Exception {
                return new Tuple2(o._2, o._1);
            }
        });
        JavaPairRDD sort = beingSort.sortByKey(false);
        JavaPairRDD sortValues = sort.mapToPair(new PairFunction<Tuple2, Integer, String>() {
            public Tuple2 call(Tuple2 o) throws Exception {
                return new Tuple2(o._2, o._1);
            }
        });
    }
}
