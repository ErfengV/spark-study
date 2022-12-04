package cn.bithachi.demo.rdd;

import cn.bithachi.demo.pojo.StaffInfo;
import cn.bithachi.demo.pojo.Student;
import cn.bithachi.demo.util.RowUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * @author:erfeng_v
 * @create: 2022-11-27 00:35
 * @Description: 奖罚区间，》》》》》
 * 都需要对其先rank一遍，然后开始进行奖罚
 * 求出奖罚区间
 * 在对应的区间内的奖罚 得出最终的钱
 *
 *
 */
public class Test {

    public static void main(String[] args) throws AnalysisException {
        String[] fields = getFields(StaffInfo.class);
        for (String field : fields) {
            System.out.println(field);
        }

        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSQLJsonSelect")
                .master("local").getOrCreate();

        /**
         *   {
         *     "score": "34",
         *     "class_name": "星期六",
         *     "id": "25",
         *     "store_code": "一月",
         *     "username": "石鸿涛"
         *   }
         */
        //向下取整系数
        // 第一名： [ 1 - radio ]
        // 第二名： [ radio+1 - radio+radio ]
        // 第三名： [ radio+radio+1 - quota ]
        Dataset<Row> json = sparkSession.read().json("other/student/stuInfo.json");
        Dataset<Student> staffInfoDataset = RowUtils.rowToObj(json, Student.class);
        staffInfoDataset.foreach(staffInfo -> {
            System.out.println(staffInfo.toString());
        });
    }

    public static String[] getFields(Object obj){
        Field[] fields = obj.getClass().getDeclaredFields();
        String[] fieldNames = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fieldNames[i] = fields[i].getName();
        }
        return fieldNames;
    }

    public static String[] getFields(Class obj){
        Field[] fields = obj.getDeclaredFields();
        String[] fieldNames = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fieldNames[i] = fields[i].getName();
        }
        return fieldNames;
    }



}
