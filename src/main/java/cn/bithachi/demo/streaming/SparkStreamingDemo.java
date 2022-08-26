package cn.bithachi.demo.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/26
 * @Description: TCP Socket数据处理Demo
 */
public class SparkStreamingDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("SparkStreamingDemo").setMaster("local[2]");
        // 1. 创建StreamingContext 按照时间间隔为5秒钟切分数据流
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 2.创建DStream,根据主机名和端口来获取tcp源的数据
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);

        // 3.操作DStream
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // 4.计算每一批次中每一个单词的数量
        JavaPairDStream<String, Integer> pairs = words.mapToPair(v1 -> new Tuple2(v1, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((v1, v2) -> v1 + v2);

        // 5.将此DStream中的每个RDD的前10个元素打印到控制台
        wordCounts.print();

        // 6.启动Spark　Streaming
        ssc.start();

        // 7.等待计算结束
        ssc.awaitTermination();
    }
}
