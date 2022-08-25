package cn.bithachi.demo.sql;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/25
 * @Description: 热点搜索词统计
 */
public class SparkSQLHotKeyword {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("SparkSQLHotKeyword")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // 测试数据
        JavaRDD<String> lines = sparkContext.textFile("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\hotkeyword.txt");

        // RDD转为 ((date,keyword),user) 元组
        JavaPairRDD<Tuple2<String,String>,String> linesRDD = lines.mapToPair(line -> {
            String[] strings = line.split(",");
            String date = strings[0];
            String keyword = strings[2];
            String user = strings[1];
            return new Tuple2(new Tuple2(date, keyword), user);
        });


        // 以date,keyword分组, 获取每一天每个搜索词被哪些用户进行了搜索
        JavaPairRDD<Tuple2<String, String>, Iterable<String>> groupRDD=linesRDD.groupByKey();

        // 获取 ((date,keyword),uv) 元组
        JavaPairRDD<Tuple2<String, String>, Integer> uvRDD = groupRDD.mapToPair(line -> {
            Tuple2<String, String> dateAndKeyword = line._1;
            Iterable<String> users = line._2;
            // 用户去重
            Set<String> distinctUsers = StreamSupport.stream(line._2.spliterator(), false).collect(Collectors.toSet());
           // 每天每个搜索词被搜索的用户数量
            int uv = distinctUsers.size();

            // 返回 ((date,keyword),uv) 元组
            return new Tuple2<>(dateAndKeyword, uv);
        });

        JavaRDD<Row> uvRowRDD = uvRDD.map(line -> RowFactory.create(line._1._1, line._1._2, Integer.valueOf(line._2)));

        // 创建数据的schema
        ArrayList<StructField> schemaFields = new ArrayList<>();
        schemaFields.add(DataTypes.createStructField("date",DataTypes.StringType,true));
        schemaFields.add(DataTypes.createStructField("keyword",DataTypes.StringType,true));
        schemaFields.add(DataTypes.createStructField("uv",DataTypes.IntegerType,true));
        StructType schema = DataTypes.createStructType(schemaFields);

        // 构建 DataFrame
        Dataset<Row> df = sparkSession.createDataFrame(uvRowRDD, schema);
        df.createTempView("date_keyword_uv");

        sparkSession.sql("select date,keyword,uv from "+
                "(select date,keyword,uv,row_number() over (partition by date order by uv desc) rank from date_keyword_uv) t "+
                "where t.rank<=3 order by date,uv desc").show();

    }
}
