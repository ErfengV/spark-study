package cn.bithachi.demo.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.spark_project.jetty.util.ArrayQueue;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Queue;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/26
 * @Description: RDD队列流 , 对队列RDD中的每个数做余数词频统计
 */
public class SparkStreamRDDQueue {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("SparkStreamingDemo").setMaster("local[2]");
        // 1. 创建StreamingContext 按照时间间隔为2秒钟切分数据流
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // 创建队列,存放RDD
        Queue<JavaRDD<Integer>> javaRDDQueue = new ArrayQueue<>();

        // 创建输入DStream
        JavaDStream<Integer> inputStream = ssc.queueStream(javaRDDQueue);
        JavaPairDStream<Integer, Integer> mapStream = inputStream.mapToPair(x -> new Tuple2(x % 2, 1));
        JavaPairDStream<Integer, Integer> reduceStream = mapStream.reduceByKey((v1, v2) -> v1 + v2);

        // 输出运算结果
        reduceStream.print();
        ssc.start();

        // 每隔一秒创建一个RDD并将其推入javaRDDQueue中
        for (int i = 0; i < 30; i++) {
            synchronized (javaRDDQueue) {
                ArrayList<Integer> integers = new ArrayList<>();
                for (int j = 0; j < 100; j++) {
                    integers.add(j);
                }
                //  1~100,生成一个RDD，2个分区
                javaRDDQueue.add(ssc.sparkContext().parallelize(integers, 2));
            }
            Thread.sleep(1000);
        }

        ssc.stop();
    }
}
