package org.llf.bigdata.study.one.producer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

/**
 * Description 类描述
 *
 * @author luolifeng
 * @version 1.0.0
 * Date 2019-10-22
 * Time 15:51
 */
public class KafkaConfig implements InitializingBean {

    @Value("${kafka.broker.list}")
    public String brokerList;

    public static final String topic = "TOPIC_LIN_LIANG";

    public final String groupId = "group.01";


    private Properties producerConfigs() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        //20M 消息缓存
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 20000000);
        //生产者空间不足时，send()被阻塞的时间，默认60s
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 6000);
        //生产者重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        //指定ProducerBatch（消息累加器中BufferPool中的）可复用大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //生产者会在ProducerBatch被填满或者等待超过LINGER_MS_CONFIG时发送
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return props;
    }


    @Bean
    public Producer<Integer, Object> getKafkaProducer() {
        //KafkaProducer是线程安全的，可以在多个线程中共享单个实例
        return new KafkaProducer<Integer, Object>(producerConfigs());
    }


    public void afterPropertiesSet() throws Exception {
        
    }
}
