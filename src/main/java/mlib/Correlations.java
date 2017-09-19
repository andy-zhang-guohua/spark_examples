package mlib;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;

import java.util.Arrays;

/**
 * 例子代码来源 : https://spark.apache.org/docs/2.2.0/mllib-statistics.html#correlations
 * <p>
 * Created by ZhangGuohua on 2017/9/19.
 */
@Slf4j
public class Correlations {

    public static void main(String[] args) {
        String master = "local";
        String appName = "Spark App -- mllib correlation";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        conf.set("spark.testing.memory", "2147480000"); // 因为jvm无法获得足够的资源
        JavaSparkContext jsc = new JavaSparkContext(conf);

        log.info("SparkConf : {}", jsc);

        JavaDoubleRDD seriesX = jsc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 3.0, 5.0));  // a series

        // must have the same number of partitions and cardinality as seriesX
        JavaDoubleRDD seriesY = jsc.parallelizeDoubles(Arrays.asList(11.0, 22.0, 33.0, 33.0, 555.0));

        // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method.
        // If a method is not specified, Pearson's method will be used by default.
        Double correlation = Statistics.corr(seriesX.srdd(), seriesY.srdd(), "pearson");
        System.out.println("Correlation is: " + correlation);

        // note that each Vector is a row and not a column
        JavaRDD<Vector> data = jsc.parallelize(
                Arrays.asList(
                        Vectors.dense(1.0, 10.0, 100.0),
                        Vectors.dense(2.0, 20.0, 200.0),
                        Vectors.dense(5.0, 33.0, 366.0)
                )
        );

        // calculate the correlation matrix using Pearson's method.
        // Use "spearman" for Spearman's method.
        // If a method is not specified, Pearson's method will be used by default.
        Matrix correlMatrix = Statistics.corr(data.rdd(), "pearson");
        System.out.println("相干矩阵 : ");
        System.out.println(correlMatrix.toString());
    }
}
