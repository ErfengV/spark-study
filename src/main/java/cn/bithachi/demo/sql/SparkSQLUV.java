package cn.bithachi.demo.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/25
 * @Description: 每日UV统计
 */
public class SparkSQLUV {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("SparkSQLWriteJdbc")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // 测试数据
        JavaRDD<String> lines = sparkContext.textFile("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\uv.txt");
        JavaRDD<Row> rowJavaRDD = lines.map(line -> {
            String[] strings = line.split(",");
            return RowFactory.create(strings[0], Integer.valueOf(strings[1]));
        });

        // 创建Schema
        List<StructField> schemaFields = new ArrayList();
        schemaFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("userID", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(schemaFields);

        Dataset<Row> df = sparkSession.createDataFrame(rowJavaRDD, schema);

        df.groupBy("date").agg(functions.countDistinct("userID").as("count")).show();

    }
}
