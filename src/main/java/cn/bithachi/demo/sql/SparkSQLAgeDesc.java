package cn.bithachi.demo.sql;

import cn.bithachi.demo.pojo.Person;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/24
 * @Description: Spark SQL的基本使用
 */
public class SparkSQLAgeDesc {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder().appName("SparkSQLAgeDesc").master("local").getOrCreate();

        // 加载数据为DataSet
        Dataset<String> stringDataset = sparkSession.read().textFile("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\person.txt");
        stringDataset.show();

        // 给Dataset添加元数据信息
        Dataset<Person> personDataset = stringDataset.map(
                (MapFunction<String, Person>) line -> {
                    String[] strings = line.split(",");
                    return new Person(Integer.valueOf(strings[0]),strings[1],Integer.valueOf(strings[2]));
                }
                , Encoders.bean(Person.class));
        personDataset.show();

        // 创建一个临时视图v_person
        personDataset.createTempView("v_person");
        Dataset<Row> dataset = sparkSession.sql("select * from v_person order by age desc");
        dataset.show();

    }
}
