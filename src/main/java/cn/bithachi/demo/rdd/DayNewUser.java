package cn.bithachi.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/24
 * @Description:  倒排索引统计每日新增用户
 *
 * 2020-01-01,user1
 * 2020-01-01,user2
 * 2020-01-01,user3
 * 2020-01-02,user1
 * 2020-01-02,user2
 * 2020-01-02,user4
 * 2020-01-03,user2
 * 2020-01-03,user5
 * 2020-01-03,user6
 */
public class DayNewUser {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DayNewUser").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        List<Tuple2<String, String>> tuple2s = new ArrayList();
//        tuple2s.add(new Tuple2("2020-01-01", "user1"));
//        tuple2s.add(new Tuple2("2020-01-01", "user2"));
//        tuple2s.add(new Tuple2("2020-01-01", "user3"));
//        tuple2s.add(new Tuple2("2020-01-02", "user1"));
//        tuple2s.add(new Tuple2("2020-01-02", "user2"));
//        tuple2s.add(new Tuple2("2020-01-02", "user4"));
//        tuple2s.add(new Tuple2("2020-01-03", "user2"));
//        tuple2s.add(new Tuple2("2020-01-03", "user5"));
//        tuple2s.add(new Tuple2("2020-01-03", "user6"));
        JavaPairRDD<String, String> dateUserRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("2020-01-01", "user1"),
                new Tuple2<String, String>("2020-01-01", "user2"),
                new Tuple2<String, String>("2020-01-01", "user3"),
                new Tuple2<String, String>("2020-01-02", "user1"),
                new Tuple2<String, String>("2020-01-02", "user2"),
                new Tuple2<String, String>("2020-01-02", "user4"),
                new Tuple2<String, String>("2020-01-03", "user2"),
                new Tuple2<String, String>("2020-01-03", "user5"),
                new Tuple2<String, String>("2020-01-03", "user6")
        ));

        // 调换顺序
        JavaPairRDD<String, String> userDateRDD = dateUserRDD.mapToPair(item -> new Tuple2<>(item._2, item._1));

        // 按用户名分组
        JavaPairRDD<String, Iterable<String>> userGroupDatesRDD = userDateRDD.groupByKey();

        // 每个用户注册日期的数量初始化为1
        JavaPairRDD<String, Integer> dayInitNum = userGroupDatesRDD.mapToPair(item -> {
            List<String> dateList = StreamSupport.stream(item._2.spliterator(), false).collect(Collectors.toList());
            String minDate = dateList.stream().min(String::compareTo).get();
            return new Tuple2<>(minDate, 1);
        });

        // 统计每个日期注册的用户数目
        Map<String, Long> dayAddUserNum = dayInitNum.countByKey();

        dayAddUserNum.forEach((key,value)-> System.out.println(key+":"+value));

        sc.close();
    }
}
