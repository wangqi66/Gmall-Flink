package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author wang
 * @create 2021-09-18 8:53
 */
public class MyKafkaUtil {

    private static String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static Properties properties = new Properties();

    private  static String deafultTopic = "dwd_default";

    static {
        properties.setProperty("bootstrap.servers", KAFKA_SERVER);
    }

    /**
     * 创建 kafkasink
     *
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    /**
     * 创建 kafka生产者sink（将数据写入kafka的各个分区主题）
     * @param
     * @return
     */

    public static <T> FlinkKafkaProducer<T> getKafkaConsumerSink(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"1");
        return new FlinkKafkaProducer<T>(deafultTopic,kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

    }

    /**
     * 获取KafkaSource的方法
     *
     * @param topic   主题
     * @param groupId 消费者组
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        //给配置信息对象添加配置项
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //获取KafkaSource
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }


}
