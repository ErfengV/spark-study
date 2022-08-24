package cn.bithachi.demo.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/24
 * @Description: 分区自动判断
 */
public class PartitionAutoInference {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("PartitionAutoInference").master("local").getOrCreate();

        Dataset<Row> dataset = sparkSession.read().format("json").load("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\people");
        dataset.printSchema();
        dataset.show();
    }
}
