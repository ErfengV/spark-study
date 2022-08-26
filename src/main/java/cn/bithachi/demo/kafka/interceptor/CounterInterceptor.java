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
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    /**
     * 发送成功的消息数量
     */
    private int successCounter = 0;
    /**
     * 发送失败的消息数量
     */
    private int errorCounter = 0;

    /**
     * 此方法在消息发送前调用
     * 此处不做处理
     *
     * @param producerRecord
     * @return 修改后的消息记录
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        System.out.println("CounterInterceptor------onSend 方法被调用");
        return producerRecord;
    }

    /**
     * 此方法在消息发送完毕后调用
     * 当发送到服务器的记录已被确认，或记录发送失败时，将调用此方法
     *
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        System.out.println("CounterInterceptor------onAcknowledgement 方法被调用");
        // 统计成功和失败的次数
        if (e == null) {
            successCounter++;
        } else {
            errorCounter++;
        }
    }

    /**
     * 当生产者关闭时调用此方法，可以在此将结果进行持久化保存
     */
    @Override
    public void close() {
        System.out.println("CounterInterceptor------close 方法被调用");
        // 打印统计结果
        System.out.println("发送成功的消息数量：" + successCounter);
        System.out.println("发送失败的消息数量：" + errorCounter);
    }

    /**
     * 获取生产者配置信息
     *
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {
        System.out.println(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }
}
