package cn.bithachi.demo.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/25
 * @Description: 开窗函数
 */
public class SparkSQLWindowFunction {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("SparkSQLWindowFunction")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // 创建测试数据
        List<String> phoneList = new ArrayList<>();
        phoneList.add("2019-06-01,A,500");
        phoneList.add("2019-06-01,B,600");
        phoneList.add("2019-06-01,C,550");
        phoneList.add("2019-06-02,A,700");
        phoneList.add("2019-06-02,B,800");
        phoneList.add("2019-06-02,C,880");
        phoneList.add("2019-06-03,A,790");
        phoneList.add("2019-06-03,B,700");
        phoneList.add("2019-06-03,C,980");
        phoneList.add("2019-06-04,A,920");
        phoneList.add("2019-06-04,B,990");
        phoneList.add("2019-06-04,C,680");
        JavaRDD<String> stringJavaRDD = sparkContext.parallelize(phoneList);
        JavaRDD<Row> rowJavaRDD = stringJavaRDD.map(line->{
            String[] split = line.split(",");
            return RowFactory.create(split[0],split[1],Integer.valueOf(split[2]));
        });

        // 创建Schema
        List<StructField> schemaFields = new ArrayList();
        schemaFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("money", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(schemaFields);

        // 转化
        Dataset<Row> df = sparkSession.createDataFrame(rowJavaRDD, schema);

        df.createTempView("t_sales");
        sparkSession.sql("select date,type,money,rank from " +
                "(select date,type,money, row_number() over (partition by type order by money desc ) rank  from t_sales) t " +
                "where t.rank<=3 ").show();
    }
}
