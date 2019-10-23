package org.llf.bigdata.study.one.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.llf.bigdata.study.one.producer.config.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Description 类描述
 *
 * @author luolifeng
 * @version 1.0.0
 * Date 2019-10-22
 * Time 16:03
 */
@Component
public class ProducerMain {
    public String topic = KafkaConfig.topic;

    @Autowired
    Producer producer;

    public void producer(String msgKey, String msg) throws Exception {

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msgKey, msg);

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println(metadata.partition() + ":" + metadata.offset());
                }else{
                    System.out.println("发生异常:" + metadata);
                }
            }
        });

    }
}
