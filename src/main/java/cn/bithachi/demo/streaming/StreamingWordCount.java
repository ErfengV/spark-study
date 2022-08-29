package cn.bithachi.demo.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/29
 * @Description: 按批次累加的实时单词计数
 */
public class StreamingWordCount {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]");
        // 1. 创建StreamingContext 按照时间间隔为1秒钟切分数据流
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(3));
        // 设置检查点目录，因为需要检查点记录历史批次处理的结果数据
        ssc.checkpoint("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\check");

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);
        // 根据空格将每一行字符串分割成单词
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        words.print();
        // 初始化每个单词为元组<word,1>
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
        wordCounts.print();
        // 更新每个单词的数量，实现批次累加
//        JavaPairDStream<String, Integer> result = wordCounts.updateStateByKey(StreamingWordCount::updateFunc);
        JavaPairDStream<String, Integer> result = wordCounts.updateStateByKey((values, state) -> {
            // 首先定义一个全局的单词计数
            // currentCount为当前批次某个单词最新的单词数量
            Integer currentCount = 0;

            // 其次判断，state是否存在，如果不存在，说明是一个key第一次出现
            // 如果存在，说明这个key之前已经统计过全局的次数了
            // lastCount 是上一批次某个单词的数量
            Integer lastCount = state.isPresent() ? state.get() : 0;

            // 接着，将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计次数
            for (Integer value : values) {
                currentCount += value;
            }

            return Optional.of(currentCount + lastCount);
        });
        result.print();

        // 启动计算
        ssc.start();
        // 等待计算结束
        ssc.awaitTermination();

    }

    private static Optional<Integer> updateFunc(List<Integer> values, Optional<Integer> state) {
        // 首先定义一个全局的单词计数
        // currentCount为当前批次某个单词最新的单词数量
        Integer currentCount = 0;

        // 其次判断，state是否存在，如果不存在，说明是一个key第一次出现
        // 如果存在，说明这个key之前已经统计过全局的次数了
        // lastCount 是上一批次某个单词的数量
        Integer lastCount = state.isPresent() ? state.get() : 0;

        // 接着，将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计次数
        for (Integer value : values) {
            currentCount += value;
        }

        return Optional.of(currentCount + lastCount);
    }


}
