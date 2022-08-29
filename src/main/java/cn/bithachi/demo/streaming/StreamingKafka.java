package cn.bithachi.demo.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/29
 * @Description: 整合Kafka开发单词计数
 */
public class StreamingKafka {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]");
        // 创建StreamingContext 按照时间间隔为1秒钟切分数据流
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(3));
        // 设置检查点目录，因为需要检查点记录历史批次处理的结果数据
        ssc.checkpoint("D:\\code\\IDEA\\SparkStudy\\src\\main\\resources\\other\\check");

        // 设置输入流的kafka主题，可以多个
        List<String> kafkaTopics = Arrays.asList("topictest");

        //kafka配置
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos7001:9092,centos7002:9092,centos7003:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "1");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // kafka不自动提交偏移量(默认为true)，由spark管理
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 创建输入DStream
        JavaInputDStream<ConsumerRecord<String, String>> inputDStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent()
                , ConsumerStrategies.Subscribe(kafkaTopics, kafkaParams));

        // 对接收到的一个DStream进行解析，取出消息记录的key和value
        JavaDStream<Tuple2<String, String>> linesDStream = inputDStream.map(record -> new Tuple2<>(record.key(),
                record.value()));

        // 默认情况下，消息内容存放在value中，取出value的值
        JavaDStream<String> wordsDStream = linesDStream.map(t -> t._2);
        // 取出每一行字符串，分割为一个个单词
        JavaDStream<String> word = wordsDStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        // 单词初始化计数为1
        JavaPairDStream<String,Integer> pair = word.mapToPair(x -> new Tuple2(x, 1));

        // 按批次累加实现单词计数
        JavaPairDStream<String, Integer> result = pair.updateStateByKey(StreamingKafka::updateFunc);
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
