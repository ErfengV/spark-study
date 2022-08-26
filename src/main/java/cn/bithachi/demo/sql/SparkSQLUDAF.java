package cn.bithachi.demo.sql;

import cn.bithachi.demo.sql.udaf.MyAverage;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/25
 * @Description: 自定义聚合函数的验证测试
 */
public class SparkSQLUDAF {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("SparkSQLUDF")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // 注册函数：
        sparkSession.udf().register("MyAverage", new MyAverage());

        Dataset<Row> df = sparkSession.read().json("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\employees.json");
        df.show();
        df.createTempView("employee");

        // 验证
        Dataset<Row> rowDataset = sparkSession.sql("select myAverage(salary) as average_salary from employee");
        rowDataset.show();

    }

}
