package cn.bithachi.demo.util;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * @author:erfeng_v
 * @create: 2022-12-03 17:58
 * @Description: row对象的工具
 */
public class RowUtils {

    /**
     * 行对象转换成目标对象
     * @param dataset
     * @param c
     * @return
     */
    public static Dataset rowToObj(Dataset<Row> dataset, Class c){
        return dataset.map((MapFunction<Row, Object>) row -> setValue(row, c.newInstance()), Encoders.bean(c));
    }

    /**
     * 循环赋值
     * @param source row对象
     * @param target 需要转换的目标对象
     * @return 返回目标对象
     */
    public static Object setValue(Row source, Object target){
        try {
            Field[] fields = target.getClass().getDeclaredFields();
            for (Field field : fields) {
                String setMethodName = "set" + (field.getName().charAt(0) + "").toUpperCase() + field.getName().substring(1);
                Method setMethod = target.getClass().getMethod(setMethodName,field.getType());
                //得到字段值
                Object rowObj = source.getAs(field.getName());
                setMethod.invoke(target,rowObj);
            }
        }catch (ReflectiveOperationException e){
            e.printStackTrace();
        }
        return target;
    }

    /**
     * 表直接转换成目标对象
     * @param sparkSession spark
     * @param tableName 表名
     * @param condition 条件值 例：where stat_date = '20221103'
     * @param c 目标值
     * @return 目标对象dataset
     */
    public static Dataset tableToObj(SparkSession sparkSession,String tableName,String condition,Class c){
        Dataset<Row> dataset = sparkSession.read().table(tableName).where(condition);
        return rowToObj(dataset,c);
    }

    public static Dataset tableToObj(SparkSession sparkSession,String tableName,Class c){
        Dataset<Row> dataset = sparkSession.read().table(tableName);
        return rowToObj(dataset,c);
    }

    /**
     * 根据注解获取目标对象
     * @param sparkSession spark
     * @param c 目标值
     * @return 目标对象dataset
     */
    public static Dataset tableToObj(SparkSession sparkSession,Class c) {
        TabName tabName = (TabName) c.getAnnotation(TabName.class);
        String table = tabName.table();
        String condition = tabName.condition();
        if("NAN".equals(condition)){
            return tableToObj(sparkSession,table,c);
        }
        return tableToObj(sparkSession,table,condition,c);
    }




}
