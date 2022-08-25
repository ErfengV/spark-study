package cn.bithachi.demo.sql;

import cn.bithachi.demo.udf.PhoneUDF;
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
 * @Description: 自定义函数phoneHide的验证测试
 */
public class SparkSQLUDF {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("SparkSQLUDF")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // 创建测试数据
        List<String> phoneList = new ArrayList<>();
        phoneList.add("13720887837");
        phoneList.add("13946852468");
        JavaRDD<String> stringJavaRDD = sparkContext.parallelize(phoneList);
        JavaRDD<Row> rowJavaRDD = stringJavaRDD.map(line -> RowFactory.create(line));

        // 创建Schema
        List<StructField> schemaFields = new ArrayList();
        schemaFields.add(DataTypes.createStructField("phone", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(schemaFields);

        // 转化
        Dataset<Row> df = sparkSession.createDataFrame(rowJavaRDD, schema);

        // 注册函数,方式一：
//        sparkSession.udf().register("phoneHide", (String phone)-> {
//            String result="手机号码错误";
//            if (phone!=null && phone.length()==11){
//                StringBuffer stringBuffer = new StringBuffer();
//                stringBuffer.append(phone.substring(0,3));
//                stringBuffer.append("****");
//                stringBuffer.append(phone.substring(7));
//                result=stringBuffer.toString();
//            }
//            return  result;
//        }, DataTypes.StringType);

        // 注册函数,方式二：
        sparkSession.udf().register("phoneHide",new PhoneUDF(), DataTypes.StringType);

        // 验证
        df.createTempView("t_phone");
        sparkSession.sql("select phoneHide(phone) as phone from t_phone").show();

    }

//    public static void main(String[] args) {
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("SparkSQLTest12")
//                .master("local")
//                .getOrCreate();
//
//        spark.udf().register("plusOne", new UDF1<Integer, Integer>() {
//            @Override
//            public Integer call(Integer x) {
//                return x + 1;
//            }
//        }, DataTypes.IntegerType);
//        spark.sql("SELECT plusOne(5)").show();
//        spark.stop();
//    }


}
