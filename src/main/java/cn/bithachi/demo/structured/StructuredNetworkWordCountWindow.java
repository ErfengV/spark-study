package cn.bithachi.demo.structured;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/30
 * @Description: Structured Streaming 窗口聚合单词计数
 */
public class StructuredNetworkWordCountWindow {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("StructuredNetworkWordCountWindow").getOrCreate();
        sparkSession.sparkContext().setLogLevel("WARN");

        Dataset<Row> lines = sparkSession.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .option("includeTimestamp", true)
                .load();

        lines.printSchema();
//        root
//                |-- value: string (nullable = true)
//                |-- timestamp: timestamp (nullable = true)

        // 将每一行字符串分割成单词，保留时间戳
        Dataset<Row> words = lines.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) row -> {
                    String value = row._1;
                    Timestamp timestamp = row._2;
                    System.out.println(value);
                    System.out.println(timestamp);

                    Stream<Tuple2<String, Timestamp>> tuple2Stream = Arrays.stream(value.split(" ")).map(word -> new Tuple2(word,
                            timestamp));
                    return tuple2Stream.iterator();
                }, Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).toDF("word", "timestamp");

        words.printSchema();
//        root
//                |-- word: string (nullable = true)
//                |-- timestamp: timestamp (nullable = true)

        //滑动间隔必须小于等于窗口长度。若不设互滑动问隔，则默认等于窗口长度
        String windowDuration = "10 seconds"; //窗口长度
        String slideDuration = "5 seconds"; //滑动间隔

        // 将数据按窗口和单词分组，并计算每组的数量
        Dataset<Row> windowCounts = words.groupBy(
                        functions.window(words.col("timestamp"), windowDuration, slideDuration),
                        words.col("word")
                ).count().orderBy("window");

        windowCounts.printSchema();
//        root
//                |-- window: struct (nullable = true)
//                |    |-- start: timestamp (nullable = true)
//                |    |-- end: timestamp (nullable = true)
//                |-- word: string (nullable = true)
//                |-- count: long (nullable = false)

        // 执行查询
        StreamingQuery query = windowCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .start();

        // 等待查询终止
        query.awaitTermination();
    }
}
