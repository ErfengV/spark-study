package cn.bithachi.demo.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/29
 * @Description: 对用户的访问日志根据设置的黑名单进行实时过滤，黑名单中的用户访问日志将不进行输出
 */
public class StreamingBlackListFilter {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("StreamingBlackListFilter").setMaster("local[2]");
        // 创建StreamingContext 按照时间间隔为3秒钟切分数据流
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(3));

        // 构建黑名单数据
        ArrayList<Tuple2<String, Boolean>> blackList = new ArrayList<>();
        blackList.add(new Tuple2<>("tom", true));
        blackList.add(new Tuple2<>("leo", true));
        JavaPairRDD<String, Boolean> blackListRDD = ssc.sparkContext().parallelizePairs(blackList);

        // 从socket获取日志数据
        JavaReceiverInputDStream<String> logsInputDStream = ssc.socketTextStream("localhost", 9999);

        // 将输入的的DStream转为元组，便于后面的连接查询   (tom,2019012 tom)
        JavaPairDStream<String, String> tuple2LogDStream =
                logsInputDStream.mapToPair(line -> new Tuple2<String, String>(line.split(" ")[1], line));

        // 将输入DStream进行转换操作,左外连接黑名单数据并过滤掉黑名单用户数据
        JavaDStream<String> resultDStream = tuple2LogDStream.transform(rdd -> {
            // 用户访问数据与黑名单数据进行左外连接
            JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> leftJoinRDD = rdd.leftOuterJoin(blackListRDD);

            // 过滤算子filter(func)，保留函数func结果为true的元素
            JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = leftJoinRDD.filter(line -> {
                System.out.println(line + "-----------" + line._2._2.orElse(false));
                // (leo, (20191012 leo, Some (true))------false
                // (jack, (20191012 jack, None) )------true

                // 能取到值说明在黑名单内，因此返回false将其过滤掉
                // 取不到值返回true，保留该值
                // orElse 没有值就返回false
                if (line._2._2.orElse(false)) {
                    return false;
                } else {
                    return true;
                }
            });

            // 取出原始用户访问数据
            JavaRDD<String> result = filterRDD.map(line -> line._2._1);
            return result;
        });

        resultDStream.print();
        // 启动计算
        ssc.start();
        // 等待计算结束
        ssc.awaitTermination();
    }
}
