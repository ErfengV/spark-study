package cn.bithachi.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/23
 * @Description:
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\sort.txt");

        // 将RDD转为<SecondarySortKey, String>元组
        JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(line -> {
            String[] lineSplits = line.split(" ");
            SecondarySortKey key = new SecondarySortKey(Integer.valueOf(lineSplits[0]), Integer.valueOf(lineSplits[1]));
            return new Tuple2(key, line);
        });

        // 按照元组的key(SecondarySortKey)进行排序
        JavaPairRDD<SecondarySortKey, String> sortedPairs = pairs.sortByKey();

        // 取排序后的每个元组的第二个值
        JavaRDD<String> stringJavaRDD = sortedPairs.map(item -> item._2);

        // 打印结果
        stringJavaRDD.foreach(e-> System.out.println(e));

        sc.close();
    }
}
