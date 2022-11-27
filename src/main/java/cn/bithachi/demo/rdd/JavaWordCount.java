package cn.bithachi.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/15
 * @Description: 单词计数
 */
public final class JavaWordCount {
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
        JavaRDD<String> linesRDD = sparkContext.textFile("other/words.txt");

        // 将RDD的每个元素按照空格进行拆分，并将结果合并为一个新的RDD
        JavaRDD<String> wordRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\\s+")).iterator();
            }
        });

        // 将RDD中的每个单词和数字1放到一个元组里，及（word,1）
        JavaPairRDD<String, Integer> paresRDD = wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // 对单词根据Key进行聚合，对相同的key进行value累加
        JavaPairRDD<String, Integer> wordCountsRDD = paresRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });


        // 转换为数字在前，单词在后，再进行sort排序
        JavaPairRDD<Integer, String> wordCountsSortRDD = wordCountsRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        // 按照单词数量降序排列
        JavaPairRDD<Integer, String> sortedWOrds = wordCountsSortRDD.sortByKey(false);

        // 保存结果到指定的路径（取程序运行时传入的第二个参数）
        sortedWOrds.saveAsTextFile("output");

        // 停止SparkContext，结束该任务
        sparkContext.stop();
    }
}