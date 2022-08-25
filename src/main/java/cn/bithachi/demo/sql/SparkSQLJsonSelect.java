package cn.bithachi.demo.sql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/24
 * @Description: 读取JSON数据集进行JOIN查询
 */
public class SparkSQLJsonSelect {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSQLJsonSelect")
                .master("local").getOrCreate();

        Dataset<Row> stuInfo = sparkSession.read().json("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\student\\stuInfo.json");
        stuInfo.createTempView("user_info");
        stuInfo.show();

        Dataset<Row> stuScore = sparkSession.read().json("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\student\\stuScore.json");
        stuScore.createTempView("user_score");
        stuScore.show();

        Dataset<Row> dataset = sparkSession.sql("SELECT i.name,c.grade FROM user_info i " + "JOIN user_score c ON i.name=c.name");
        dataset.show();

    }
}
