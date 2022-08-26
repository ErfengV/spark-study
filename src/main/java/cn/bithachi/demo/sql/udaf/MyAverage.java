package cn.bithachi.demo.sql.udaf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/25
 * @Description:
 */
public class MyAverage extends UserDefinedAggregateFunction {

    /**
     * StructType表示此聚合函数的输入参数的数据类型
     */
    @Override
    public StructType inputSchema() {
        List<StructField> schemaFields = new ArrayList();
        schemaFields.add(DataTypes.createStructField("inputColumn", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(schemaFields);
        return schema;
    }

    /**
     * StructType表示聚合缓冲区中值的数据类型
     */
    @Override
    public StructType bufferSchema() {
        List<StructField> schemaFields = new ArrayList();
        schemaFields.add(DataTypes.createStructField("sum", DataTypes.DoubleType, true));
        schemaFields.add(DataTypes.createStructField("count", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(schemaFields);
        return schema;
    }

    /**
     * 返回值的数据类型
     */
    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    /**
     * 针对给定的同一组输入，聚合函数是否返回相同的结果，通常为true
     */
    @Override
    public boolean deterministic() {
        return true;
    }

    /**
     * 初始化聚合运算中间结果
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,0D); // sum初始值
        buffer.update(1,0D); // count初始值
    }

    /**
     * 由于参与聚合的数据会依次输入聚合函数，因此每当向聚合函数输入新的数据时，都会调用该函
     * 数更新聚合中间结果
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        if (!input.isNullAt(0)){
            buffer.update(0,buffer.getDouble(0)+input.getDouble(0)); // sum
            buffer.update(1,buffer.getDouble(1)+1); // count
        }
    }

    /**
     * 合井多个分区的buffer中间结果(分布式计算，参与聚合的数据存储于多个分区，每个分区都
     * 会产生buffer中间结果)
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            buffer1.update(0,buffer1.getDouble(0)+buffer2.getDouble(0));
            buffer1.update(1,buffer1.getDouble(1)+buffer2.getDouble(1));
    }

    /**
     * 计算最终结果：平均值=数据总和/数据总量
     */
    @Override
    public Object evaluate(Row buffer) {
       return   buffer.getDouble(0)/buffer.getDouble(1);
    }
}
