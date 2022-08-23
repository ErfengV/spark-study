package cn.bithachi.demo;

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
 * @Description: 分组求TopN
 */
public class GroupTopN {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("GroupTopN").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\score.txt");

        //拆分为JavaPairRDD<String, Integer> = <姓名,成绩>
        JavaPairRDD<String, Integer> ns = lines.mapToPair(s ->
                new Tuple2<>(s.split(",")[0], Integer.valueOf(s.split(",")[1]))
        );

        //根据Key姓名分组
        JavaPairRDD<String, Iterable<Integer>> nsPairs = ns.groupByKey();
        //根据Key姓名排序，升序
        JavaPairRDD<String, Iterable<Integer>> nsPairsSort = nsPairs.sortByKey();

        //遍历取出Top3
        nsPairsSort.foreach(nsPair -> {
            String name = nsPair._1();
            Iterable<Integer> gradesIterable = nsPair._2();
            List<Integer> gradeList = StreamSupport.stream(gradesIterable.spliterator(), false).collect(Collectors.toList());
            // Collections.sort(gradeList, (o1, o2) -> o2 - o1);
            gradeList.sort((o1,o2)->o2-o1);
            System.out.println(name+": "+gradeList.subList(0, 3));
        });
        sc.close();
    }
}
