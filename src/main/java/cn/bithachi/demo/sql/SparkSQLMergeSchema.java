package cn.bithachi.demo.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/24
 * @Description: Schema合并
 */
public class SparkSQLMergeSchema {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSQLMergeSchema")
                .config("spark.sql.parquet.mergeSchema",true)
                .master("local").getOrCreate();

        Dataset<Row> dataset1 = sparkSession.read().format("json").load("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\student\\s1.json");
        dataset1.write().mode(SaveMode.Append).save("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\students");

        Dataset<Row> dataset2 = sparkSession.read().format("json").load("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\student\\s2.json");
        dataset2.write().mode(SaveMode.Append).save("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\students");

        Dataset<Row> mergeDF = sparkSession.read().option("mergeSchema", true).parquet("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\students");
        mergeDF.printSchema();
        mergeDF.show();

    }
}
