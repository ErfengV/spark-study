package cn.bithachi.demo.sql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;

import java.util.Properties;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Scope.col;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/24
 * @Description:
 */
public class SparkSQLFunction {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSQLJdbc")
                .master("local").getOrCreate();

        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "123456");
        Dataset<Row> load = sparkSession.read().jdbc("jdbc:mysql://localhost:3306/test", "user", properties);
        load.show();

        // name转大写
        load.select(functions.upper(functions.col("name")).as("name"), functions.col("age")).show();

        load.createTempView("temp");
        sparkSession.sql("select upper(name) as name,age from temp").show();

        // age+1
        load.select(functions.col("name"),functions.col("age").plus(1).as("age")).show();

        // age>20
        load.filter(functions.col("age").geq(20)).show();

        // 分组统计
        load.groupBy("age").count().show();

    }
}
