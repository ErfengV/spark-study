package cn.bithachi.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/23
 * @Description:
 */
public class AverageScore {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AverageScore").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\a");
        // 将RDD中的元素转为<key,value>==<name,score>
        JavaPairRDD<String, Integer> tupleRDD = lines.mapToPair(line -> {
            String[] lineSplits = line.split(" ");
            return new Tuple2(lineSplits[0], Integer.valueOf(lineSplits[1]));
        });

        // 根据name进行分组
        JavaPairRDD<String, Iterable<Integer>> groupRDD = tupleRDD.groupByKey();

        // 计算每个学生的平均分
        JavaPairRDD<String, Integer> resultRDD = groupRDD.mapToPair(rdd -> {
            String name = rdd._1;
            List<Integer> gradeList = StreamSupport.stream(rdd._2.spliterator(), false).collect(Collectors.toList());
            // double asDouble = gradeList.stream().mapToInt(Integer::intValue).average().getAsDouble();
            Integer average= gradeList.stream().collect(Collectors.averagingInt(Integer::intValue)).intValue();
            System.out.println(name+": " +average);
            return new Tuple2<>(name, average);
        });

        // 输出到文件
        resultRDD.saveAsTextFile("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\output");

        sc.close();
    }
}
