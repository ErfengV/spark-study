package cn.bithachi.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/26
 * @Description: kafka生产者
 */
public class MyProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置生产者Broker服务器地址
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"centos7001:9092,centos7002:9092,centos7003:9092");
        // 设置序列化key程序类
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置序列化value程序类，不一定是Integer，可以是String
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

        // 定义消息生产者
        KafkaProducer<String, Integer> kafkaProducer = new KafkaProducer<>(properties);

        // 发送消息方式一：消息发送给服务器即发送完成，而不管消息是否送达
        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<String,Integer>("topictest","hello kafka "+i,i));
        }

        // 发送消息方式二：同步发送
//        for (int i = 0; i < 10; i++) {
//            try {
//                // RecordMetadata对象，可以利用该对象获取消息的偏移量等
//                RecordMetadata recordMetadata = kafkaProducer.send(new ProducerRecord<String, Integer>("topictest", "hello kafka " + i, i)).get();
//            } catch (Exception e) {
//               e.printStackTrace();
//            }
//        }

        // 发送消息方式三：异步发送
//        for (int i = 0; i < 10; i++) {
//            try {
//               kafkaProducer.send(new ProducerRecord<String, Integer>("topictest", "hello kafka " + i, i),new Callback(){
//                   @Override
//                   public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                       if (e!=null){
//                           e.printStackTrace();
//                       }
//                   }
//               });
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }

        kafkaProducer.close();

    }
}
