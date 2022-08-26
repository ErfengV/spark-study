package cn.bithachi.demo.kafka.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/26
 * @Description: 测试生产者拦截器
 */
public class MyInterceptorProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置生产者Broker服务器地址
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos7001:9092,centos7002:9092,centos7003:9092");
        // 设置序列化key程序类
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置序列化value程序类
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置拦截器链,需指定全路径
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Arrays.asList("cn.bithachi.demo.kafka.interceptor.CounterInterceptor", "cn.bithachi" +
                ".demo.kafka.interceptor.TimeInterceptor"));

        // 定义消息生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);


        // 发送消息方式，消息发送给服务器即发送完成，而不管消息是否送达
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<String,String>("topictest","hello kafka "+i));
        }

        // 调用该方法将触发拦截器的close()方法
        kafkaProducer.close();

    }
}
