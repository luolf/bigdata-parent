package org.llf.bigdata.study.one.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.llf.bigdata.study.one.consumer.config.KafkaConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Description 类描述
 *
 * @author luolifeng
 * @version 1.0.0
 * Date 2019-10-22
 * Time 16:20
 */
@Component
public class ConsumerMain implements InitializingBean {
    private String topic = KafkaConfig.topic;
    @Autowired
    KafkaConfig kafkaConfig;

    @Override
    public void afterPropertiesSet() throws Exception {
        //每个线程一个KafkaConsumer实例，且线程数设置成分区数，最大化提高消费能力
        //线程数设置成分区数，最大化提高消费能力
        int consumerThreadNum = 2;
        for (int i = 0; i < consumerThreadNum; i++) {
            new KafkaConsumerThread(kafkaConfig.consumerConfigs(), topic).start();
        }
    }

    public class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;

        public KafkaConsumerThread(Properties props, String topic) {
            this.kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(record.key() +"--------: "+record.value());
                    }
                }
            } catch (Exception e) {
            } finally {
                kafkaConsumer.close();
            }
        }
    }
}
