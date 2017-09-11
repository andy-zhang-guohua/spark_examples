import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;


/**
 * Created by ZhangGuohua on 2017/9/11.
 */
@Slf4j
public class RDDFromJavaList {
    // 完成对所有数求和
    public static void main(String[] args) {

        String master = "local";
        String appName = "First Spark App";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        conf.set("spark.testing.memory", "2147480000"); // 因为jvm无法获得足够的资源
        JavaSparkContext sc = new JavaSparkContext(conf);

        log.info("SparkConf : {}", sc);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        //并行集合，是通过对于驱动程序中的集合调用JavaSparkContext.parallelize 来构建的RDD
        JavaRDD<Integer> distData = sc.parallelize(data);

        JavaRDD<Integer> lineLengths = distData.map(new GetLength());

        // 运行reduce 这是一个动作action 这时候，spark才将计算拆分成不同的task，
        // 并运行在独立的机器上，每台机器运行他自己的map部分和本地的reduction，并返回结果集给去驱动程序
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        log.info("总和" + totalLength);
        // 为了以后复用 持久化到内存...
        lineLengths.persist(StorageLevel.MEMORY_ONLY());

    }

    // 定义map函数
    static class GetLength implements Function<Integer, Integer> {

        @Override
        public Integer call(Integer a) throws Exception {
            return a;
        }
    }
}
