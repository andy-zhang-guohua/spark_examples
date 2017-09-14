package basic;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * 蒙特卡罗算法应用 : 求圆周率 Pi
 * <p>
 * 思想 :
 * ======== 一个半径r=1的圆形，其面积为：S=Pi∗r^2=Pi,假设该圆圆心在直角平面坐标系上的原点，则该圆在第一象限的面积为 Pi/4 ;
 * ======== 一个边长r=1的正方形，其面积为：S=r^2=1 , 假设该正方形在直角平面坐标系上的坐标为 [(0,0),(1,1)];
 * 那么如果均匀的向正方形内撒点，那么落入以上4/1圆的点数与全部点数的比例应该为 Pi/4，根据概率统计的规律，只要撒的点足够多，那么便会得到圆周率Pi的非常近似的值。
 * <p>
 * 蒙特卡罗算法关键
 * === 使用蒙特卡罗算法计算圆周率有下面两个关键点：
 * ======1. 均匀撒点：用随机函数来实现，产生[0，1)之间随机的坐标值(x,y)；
 * ======2. 区域判断：位于圆内的点的特性是其与圆心的距离小于等于1，这样可用x^2+y^2<=1来判断；
 * <p>
 * 概率算法基本思想
 * <p>
 * 概率算法是依照概率统计的思路来求解问题的算法，它往往不能得到问题的精确解。
 * 执行的基本过程如下：
 * <p>
 * ======1. 将问题转换为相应的几何图形S，S的面积是容易计算的，问题的结果往往对应几何图形某一部分S1的面积；
 * ======2. 然后，向几何图形中随机撒点；
 * ======3. 统计几何图形S和S1中的点数，根据面积关系得结果；
 * ======4. 判断精度，满足要求则输出，不满足则返回(2)；
 * 概率算法大致分为以下4类：
 * <p>
 * ======1. 数值概率算法
 * ======2. 蒙特卡罗（Monte Carlo）算法
 * ======3. 拉斯维加斯（Las Vegas）算法
 * ======4. 舍伍德（sherwood）算法
 * <p>
 * Created by ZhangGuohua on 2017/9/14.
 */
@Slf4j
public class MonteCarloPi {
    // 完成对所有数求和
    public static void main(String[] args) {

        String master = "local";
        String appName = "Spark App -- Pi";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        conf.set("spark.testing.memory", "2147480000"); // 因为jvm无法获得足够的资源
        JavaSparkContext sc = new JavaSparkContext(conf);

        log.info("SparkConf : {}", sc);

        final int NUM_SAMPLES = 10000000;
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
