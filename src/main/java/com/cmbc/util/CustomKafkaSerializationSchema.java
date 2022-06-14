package com.cmbc.util;/*
 * @Package com.cmbc.util
 * @author wang shuangli
 * @date 2022-05-14 17:25
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */


import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class CustomKafkaSerializationSchema implements KafkaSerializationSchema<String> {

    private static final long serialVersionUID = 8497940668660042203L;

    private String topic;

    public CustomKafkaSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(final String element, final Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(topic, element.getBytes());
    }

}
