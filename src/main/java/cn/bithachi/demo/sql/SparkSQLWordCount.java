package cn.bithachi.demo.sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/25
 * @Description: 单词计数
 */
public class SparkSQLWordCount {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("SparkSQLWindowFunction")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // 读取每一行
        Dataset<String> lines = sparkSession.read().textFile("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\words.txt");
        lines.show();

        // 每一行转化为一个一个的单词
        Dataset<String> words = lines.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split("\\s+")).iterator(), Encoders.STRING());
        words.show();

        // 修改列名
        Dataset<Row> df = words.withColumnRenamed("value", "word");
        df.show();

        df.createTempView("v_words");
        sparkSession.sql("select word,count(*) count from v_words group by word order by count").show();
    }
}
