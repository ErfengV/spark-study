package cn.bithachi.demo.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/24
 * @Description: jdbc链接mysql查询数据
 */
public class SparkSQLJdbc {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSQLJdbc")
                .master("local").getOrCreate();

//        Dataset<Row> load = sparkSession.read().format("jdbc")
//                .option("url", "jdbc:mysql://localhost:3306/test")
//                .option("driver", "com.mysql.jdbc.Driver")
//                .option("query", "select * from user ")
//                .option("user", "root")
//                .option("password", "123456")
//                .load();

        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","123456");
        Dataset<Row> load = sparkSession.read().jdbc("jdbc:mysql://localhost:3306/test","user",properties);
        Dataset<Row> a = load.select("name", "sex");
        Dataset<Row> b = load.select("name","sex").where("name ='王五'");
        a.show();
        b.show();

        load.show();
    }
}
