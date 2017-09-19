package mlib;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import java.util.Arrays;

/**
 * 例子代码来源 : https://spark.apache.org/docs/2.2.0/mllib-statistics.html#summary-statistics
 * <p>
 * Created by ZhangGuohua on 2017/9/19.
 */
@Slf4j
public class SummaryStatistics {

    public static void main(String[] args) {
        String master = "local";
        String appName = "Spark App -- mllib summary statistics";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        conf.set("spark.testing.memory", "2147480000"); // 因为jvm无法获得足够的资源
        JavaSparkContext jsc = new JavaSparkContext(conf);

        log.info("SparkConf : {}", jsc);

        JavaRDD<Vector> mat = jsc.parallelize(
                Arrays.asList(
                        Vectors.dense(1.0, 10.0, 100.0),
                        Vectors.dense(2.0, 20.0, 200.0),
                        Vectors.dense(3.0, 30.0, 300.0)
                )
        ); // an RDD of Vectors

        // Compute column summary statistics.
        MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
        System.out.println(summary.mean());  // a dense vector containing the mean value for each column
        System.out.println(summary.variance());  // column-wise variance, 列样本方差
        System.out.println(summary.numNonzeros());  // number of nonzeros in each column
    }
}
