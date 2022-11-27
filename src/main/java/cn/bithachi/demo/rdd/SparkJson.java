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
                ",rank() over(partition by store_code order by score desc) as rank\n" +
                "from stu \n" +
                "),\n" +
                "\n" +
                "-- 找出每个门店下的员工总人数。 \n" +
                "people_count_tab as (\n" +
                "select \n" +
                "count(1) as people_count,store_code \n" +
                "from stu group by store_code\n" +
                "),\n" +
                "\n" +
                "---------- 计算。营销能手奖200\n" +
                "-- 当总人数大于20人时 计算出相应的奖励人数\n" +
                "quota_gt20_tab as(\n" +
                "select \n" +
                "floor(people_count * 0.3) as quota\n" +
                ",store_code\n" +
                ",floor(floor(people_count * 0.3) * 0.3) as first\n" +
                ",floor(floor(people_count * 0.3) * 0.3) * 2 as second\n" +
                "from people_count_tab where people_count > 20\n" +
                "),\n" +
                "-- 只计算大于20人的\n" +
                "count_gt20_ans as ( \n" +
                "select  \n" +
                "case when t1.rank <= t2.first then 150\n" +
                "\t when t1.rank <=t2.second then 100\n" +
                "\t when t1.rank <= t2.quota then 50\n" +
                "else 0 end as award,\n" +
                "t1.username,\n" +
                "t1.store_code,\n" +
                "t1.class_name\n" +
                "from rank_tab t1 inner join quota_gt20_tab t2 on t1.store_code = t2.store_code\n" +
                "),\n" +
                "-- 当营销员的人数在3 - 5 \n" +
                "quota_lt3_5_tab as (\n" +
                "select \n" +
                "1 as quota\n" +
                ",store_code \n" +
                "from people_count_tab where people_count between 3 and 5\n" +
                "),\n" +
                "-- 只计算 3 - 5人的\n" +
                "count_lt3_5_ans as (\n" +
                "select \n" +
                "case when t1.rank = t2.quota then 150 \n" +
                "else 0 end as award,\n" +
                "t1.username,\n" +
                "t1.store_code,\n" +
                "t1.class_name\n" +
                "from rank_tab t1 inner join quota_lt3_5_tab t2 on t1.store_code = t2.store_code\n" +
                "),\n" +
                "\n" +
                "\n" +
                "-- 当营销员的人数在6 - 8 \n" +
                "quota_lt6_8_tab as (\n" +
                "select \n" +
                "2 as quota \n" +
                ",1 as first\n" +
                ",store_code\n" +
                "from people_count_tab where people_count between 6 and 8\n" +
                "),\n" +
                "-- 只计算 6 - 8人的\n" +
                "count_lt6_8_ans as (\n" +
                "select \n" +
                "case when t1.rank = t2.first then 150 \n" +
                "\t when t1.rank = t2.quota then 120\n" +
                "else 0 end as award\n" +
                ",t1.username\n" +
                ",t1.store_code\n" +
                ",t1.class_name\n" +
                "from rank_tab t1 inner join quota_lt6_8_tab t2 on t1.store_code = t2.store_code\n" +
                "),\n" +
                "\n" +
                "\n" +
                "\n" +
                "-- 当营销员的人数在9 - 12 \n" +
                "quota_lt9_12_tab as (\n" +
                "select \n" +
                "3 as quota \n" +
                ",1 as first\n" +
                ",2 as second\n" +
                ",store_code\n" +
                "from people_count_tab where people_count between 9 and 12\n" +
                "),\n" +
                "-- 只计算 9 - 12人的\n" +
                "count_lt9_12_ans as (\n" +
                "select \n" +
                "case when t1.rank = t2.first  then 150 \n" +
                "\t when t1.rank = t2.second then 120\n" +
                "\t when t1.rank = t2.quota  then 100\n" +
                "else 0 end as award\n" +
                ",t1.username\n" +
                ",t1.store_code\n" +
                ",t1.class_name\n" +
                "from rank_tab t1 inner join quota_lt9_12_tab t2 on t1.store_code = t2.store_code\n" +
                "),\n" +
                "\n" +
                "-- 当营销员人数在13 - 15 人时\n" +
                "quota_lt13_15_tab as (\n" +
                "select \n" +
                "4 as quota \n" +
                ",1 as first\n" +
                ",2 as second\n" +
                ",3 as third\n" +
                ",store_code\n" +
                "from people_count_tab where people_count between 13 and 15\n" +
                "),\n" +
                "-- 只计算 13 - 15人的\n" +
                "count_lt13_15_ans as (\n" +
                "select \n" +
                "case when t1.rank = t2.first  then 150 \n" +
                "\t when t1.rank = t2.second then 120\n" +
                "\t when t1.rank = t2.third  then 100\n" +
                "\t when t1.rank = t2.quota  then 80\n" +
                "else 0 end as award\n" +
                ",t1.username\n" +
                ",t1.store_code\n" +
                ",t1.class_name\n" +
                "from rank_tab t1 inner join quota_lt13_15_tab t2 on t1.store_code = t2.store_code\n" +
                "),\n" +
                "\n" +
                "\n" +
                "-- 当营销员人数在16 - 20人时\n" +
                "quota_lt16_20_tab as (\n" +
                "select \n" +
                "5 as quota \n" +
                ",1 as first\n" +
                ",2 as second\n" +
                ",3 as third\n" +
                ",4 as fourth\n" +
                ",store_code\n" +
                "from people_count_tab where people_count between 16 and 20\n" +
                "),\n" +
                "-- 只计算 16 - 20人的\n" +
                "count_lt16_20_ans as (\n" +
                "select \n" +
                "case when t1.rank = t2.first  then 150 \n" +
                "\t when t1.rank = t2.second then 120\n" +
                "\t when t1.rank = t2.third  then 100\n" +
                "\t when t1.rank = t2.fourth then 80\n" +
                "\t when t1.rank = t2.quota  then 50\n" +
                "else 0 end as award\n" +
                ",t1.username\n" +
                ",t1.store_code\n" +
                ",t1.class_name\n" +
                "from rank_tab t1 inner join quota_lt16_20_tab t2 on t1.store_code = t2.store_code\n" +
                ")\n" +
                "select * from count_gt20_ans\n"
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














