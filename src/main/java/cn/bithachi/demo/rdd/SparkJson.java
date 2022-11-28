package cn.bithachi.demo.rdd;

import cn.bithachi.demo.pojo.ScoreAward;
import cn.bithachi.demo.pojo.Staff;
import cn.bithachi.demo.pojo.Student;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author:xxxxx
 * @create: 2022-11-26 22:54
 * @Description: 测试
 */
public class SparkJson {

    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSQLJsonSelect")
                .master("local").getOrCreate();

        /**
         *   {
         *     "score": "34",
         *     "class_name": "星期六",
         *     "id": "25",
         *     "store_code": "一月",
         *     "username": "石鸿涛"
         *   }
         */
        //向下取整系数
        // 第一名： [ 1 - radio ]
        // 第二名： [ radio+1 - radio+radio ]
        // 第三名： [ radio+radio+1 - quota ]
        Dataset<Row> json = sparkSession.read().option("header",true).csv("other/test/score.csv");
        json.createTempView("stu");

        sparkSession.sql("-- 营销能手奖\n" +
                "-- 求出该门店下所有员工的排名\n" +
                "with rank_tab as (\n" +
                "select score,class_name,id,store_code,username\n" +
                ",rank() over(partition by store_code order by score desc) as rank \n" +
                "from stu \n" +
                "),\n" +
                "\n" +
                "-- 找出每个门店下的员工总人数。 \n" +
                "people_count_tab as (\n" +
                "select \n" +
                "count(1) as people_count,store_code \n" +
                "from stu group by store_code\n" +
                "),\n" +
                "-- 当营销员的人数在3 - 5 \n" +
                "b_quota_lt3_5_tab as (\n" +
                "select \n" +
                "1 as quota\n" +
                ",store_code \n" +
                "from people_count_tab where people_count between 3 and 5\n" +
                "),\n" +
                "rank_one_lt3_5_tab as (\n" +
                "select t1.store_code,t1.score from rank_tab t1 inner join b_quota_lt3_5_tab t2 on t1.store_code = t2.store_code\n" +
                "where t1.rank = 1\n" +
                "),\n" +
                "rank_two_lt3_5_tab as (\n" +
                "select t1.store_code,t1.score from rank_tab t1 inner join b_quota_lt3_5_tab t2 on t1.store_code = t2.store_code\n" +
                "where t1.rank = 2\n" +
                "),\n" +
                "rank_res_lt3_5_tab as (\n" +
                "select\n" +
                "case when t1.score > t2.score * 0.2 then 400\n" +
                "\t when t1.score < t2.score * 0.1 then 200\n" +
                "else 300 end as award,\n" +
                "t1.store_code  \n" +
                "from rank_one_lt3_5_tab t1 inner join rank_two_lt3_5_tab t2 on t1.store_code = t2.store_code\n" +
                "),\n" +
                "-- 只计算 3 - 5人的\n" +
                "b_count_lt3_5_ans as (\n" +
                "select \n" +
                "case when t1.rank = t2.quota  then t3.award \n" +
                "else 0 end as award,\n" +
                "t1.username,\n" +
                "t1.store_code,\n" +
                "t1.class_name\n" +
                "from rank_tab t1 \n" +
                "inner join b_quota_lt3_5_tab t2 on t1.store_code = t2.store_code\n" +
                "left join rank_res_lt3_5_tab t3 on t1.store_code = t3.store_code\n" +
                ")\n" +
                "select * from b_count_lt3_5_ans\n" +
                "\n"
        ).write().format("csv").mode("overwrite").save("other/test_ans");


//        JavaPairRDD<String, Iterable<Staff>> class_name = json.map((MapFunction<Row, Staff>) value -> {
//            Staff staff = new Staff();
//            staff.setClass_name(value.getAs("class_name"));
//            staff.setUserId(value.getAs("username"));
//            staff.setClass_name(value.getAs("level"));
//            staff.setScore(value.getAs("score"));
//            return staff;
//        }, Encoders.bean(Staff.class)).toJavaRDD().groupBy((Function<Staff, String>) Staff::getClass_name);

//        RelationalGroupedDataset class_name1 = staffDataset.groupBy("class_name");



//        JavaPairRDD<String, ScoreAward> stringScoreAwardJavaPairRDD =
//                class_name.flatMapValues((Function<Iterable<Staff>, Iterable<ScoreAward>>) v1 -> {
//                    List<ScoreAward> awards = new ArrayList<>();
//                    List<Staff> collect = StreamSupport.stream(v1.spliterator(), true)
//                            .sorted(Comparator.comparing(Staff::getScore)).collect(Collectors.toList());
//                    //得出总人数
//                    int allCount = collect.size();
//                    if(allCount < 20){
//                        return awards;
//                    }
//                    //求出应该奖励的名额
//                    double quota = Math.floor(allCount * 0.3);
//
//
//                    double ratio = Math.floor(quota / 3);
//
//
//
//                    return awards;
//        });



    }
}














