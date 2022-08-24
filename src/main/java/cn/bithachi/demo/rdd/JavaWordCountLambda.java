package cn.bithachi.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/15
 * @Description: 单词统计
 */
public class JavaWordCountLambda {
    public static void main(String[] args) {
        // 创建SparkConf对象,存储应用程序的配置信息
        SparkConf sparkConf = new SparkConf();
        // 设置应用程序名称
        sparkConf.setAppName("Spark-WordCount");
        // 设置集群Master节点访问地址
        sparkConf.setMaster("local");

        // 创建SparkContext对象，搞对象是提交Spark应用程序的入口
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 读取指定路径（取程序执行时传入的第一个参数）中文件的内容，生成一个RDD集合
        JavaRDD<String> linesRDD = sparkContext.textFile("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\words.txt");

        // 将RDD的每个元素按照空格进行拆分，并将结果合并为一个新的RDD
        JavaRDD<String> wordRDD = linesRDD.flatMap( s -> Arrays.asList(s.split("\\s+")).iterator());

        // 将RDD中的每个单词和数字1放到一个元组里，及（word,1）
        JavaPairRDD<String, Integer> paresRDD = wordRDD.mapToPair( s -> new Tuple2<>(s, 1));

        // 对单词根据Key进行聚合，对相同的key进行value累加
        JavaPairRDD<String, Integer> wordCountsRDD = paresRDD.reduceByKey( (integer, integer2) -> integer + integer2);


        // 转换为数字在前，单词在后，再进行sort排序
        JavaPairRDD<Integer, String> wordCountsSortRDD = wordCountsRDD.mapToPair( stringIntegerTuple2 -> stringIntegerTuple2.swap());

        // 按照单词数量降序排列
        JavaPairRDD<Integer, String> sortedWOrds = wordCountsSortRDD.sortByKey(false);

        // 保存结果到指定的路径（取程序运行时传入的第二个参数）
        sortedWOrds.saveAsTextFile("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\output");

        // 停止SparkContext，结束该任务
        sparkContext.stop();
    }
}
