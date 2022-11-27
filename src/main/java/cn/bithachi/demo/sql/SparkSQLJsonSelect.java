package cn.bithachi.demo.sql;

import cn.bithachi.demo.pojo.Student;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

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

        Dataset<Row> stuInfo = sparkSession.read().json("other/student/stuInfo.json");
        Dataset<Student> studentDataset = stuInfo.map((MapFunction<Row , Student >) line ->
                new Student(line.getAs("name"), line.getAs("age"),line.getAs("sex")), Encoders.bean(Student.class));
        studentDataset.createTempView("user_info");
        stuInfo.show();

        Dataset<Row> stuScore = sparkSession.read().json("other/student/stuScore.json");
        stuScore.createTempView("user_score");
        stuScore.show();


        Dataset<Row> dataset = sparkSession.sql("SELECT i.name,c.grade FROM user_info i " + "JOIN user_score c ON i.name=c.name");
        dataset.show();

    }
}
