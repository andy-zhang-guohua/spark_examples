package basic;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ZhangGuohua on 2017/9/14.
 */
@Slf4j
public class Pi {
    // 完成对所有数求和
    public static void main(String[] args) {

        String master = "local";
        String appName = "Spark App -- Pi";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        conf.set("spark.testing.memory", "2147480000"); // 因为jvm无法获得足够的资源
        JavaSparkContext sc = new JavaSparkContext(conf);

        log.info("SparkConf : {}", sc);

        final int NUM_SAMPLES = 100000000;
        List<Integer> listIntegers = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            listIntegers.add(i);
        }

        long count = sc.parallelize(listIntegers).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x * x + y * y < 1;
        }).count();

        double pi = 4.0 * count / NUM_SAMPLES;
        log.info("Pi is roughly {}", pi);
    }
}
