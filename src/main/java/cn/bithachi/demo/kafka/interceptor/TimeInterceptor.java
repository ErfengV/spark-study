package cn.bithachi.demo.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/26
 * @Description: 时间戳拦截器
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {
    /**
     * 此方法在消息发送前调用
     * 对原消息记录进行修改，在消息内容最前边添加时间戳
     *
     * @param producerRecord
     * @return 修改后的消息记录
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        System.out.println("TimeInterceptor------onSend 方法被调用");
        // 创建一条新的消息记录，将时间戳加入消息内容的最前边
        return new ProducerRecord<String, String>(producerRecord.topic(), producerRecord.key(),
                System.currentTimeMillis() + "," + producerRecord.value().toString());
    }

    /**
     * 此方法在消息发送完毕后调用
     * 当发送到服务器的记录已被确认，或记录发送失败时，将调用此方法
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        System.out.println("TimeInterceptor------onAcknowledgement 方法被调用");
    }

    /**
     * 当拦截器关闭时调用此方法
     */
    @Override
    public void close() {
        System.out.println("TimeInterceptor------close 方法被调用");
    }

    /**
     * 获取生产者配置信息
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {
        System.out.println(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }
}
