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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/25
 * @Description: 数据写入MySQL
 */
public class SparkSQLWriteJdbc {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("SparkSQLWriteJdbc")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // 创建测试数据
        List<String> userList = new ArrayList<>();
        userList.add("liyi,15");
        userList.add("lier,25");
        JavaRDD<String> lines = sparkContext.parallelize(userList);
        JavaRDD<Row> rowJavaRDD = lines.map(line -> {
            String[] strings = line.split(",");
            return RowFactory.create(UUID.randomUUID().toString(), strings[0], Integer.valueOf(strings[1]));
        });

        // 创建Schema
        List<StructField> schemaFields = new ArrayList();
        schemaFields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(schemaFields);

        Dataset<Row> df = sparkSession.createDataFrame(rowJavaRDD, schema);

        df.write().mode("append").format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/test")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "user")
                .option("user", "root")
                .option("password", "123456")
                .save();
    }
}
