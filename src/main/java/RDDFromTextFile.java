import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * 使用文件系统的文本文件构造 Spark RDD 对象，并操作
 * 目前例子文件 data.txt 放在 resources 目录下面 , 在IDE idea中运行时该文件的文件系统路径为 target/classes/data.txt
 * Created by ZhangGuohua on 2017/9/12.
 */
@Slf4j
public class RDDFromTextFile {
    // 完成对所有数求和
    public static void main(String[] args) {

        String master = "local";
        String appName = "Second Spark App";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        conf.set("spark.testing.memory", "2147480000"); // 因为jvm无法获得足够的资源
        JavaSparkContext sc = new JavaSparkContext(conf);

        log.info("SparkConf : {}", sc);

        JavaRDD<String> distFile = sc.textFile("target/classes/data.txt");
        int count_characters = distFile.map(s -> s.length()).reduce((a, b) -> a + b);

        log.info("总字符个数(不包含换行符) : {}", count_characters);
    }
}
