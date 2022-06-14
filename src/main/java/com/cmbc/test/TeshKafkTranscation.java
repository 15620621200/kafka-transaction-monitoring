package com.cmbc.test;/*
 * @Package com.cmbc.test
 * @author wang shuangli
 * @date 2022-05-19 14:25
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class TeshKafkTranscation {
    public static void main(String[] args) {
        Properties producerProps = new Properties();
//        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers().replace("PLAINTEXT://",""));
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, "192.168.138.131:9092,192.168.138.132:9092,192.168.138.133:9092");
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.setProperty(TRANSACTION_TIMEOUT_CONFIG, "300000");
        producerProps.put(TRANSACTIONAL_ID_CONFIG, "prod-0");
        KafkaProducer<String, String> producer = new KafkaProducer(producerProps);
        ProducerRecord<String, String> record = new ProducerRecord<>("something", "A message");
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(record);
        producer.commitTransaction();
    }

//    public static KafkaContainer kafka = new KafkaContainer();

    @Test
    public void testIt() {
        Properties producerProps = new Properties();
//        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers().replace("PLAINTEXT://",""));
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, "192.168.138.131:9092,192.168.138.132:9092,192.168.138.133:9092");
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.setProperty(TRANSACTION_TIMEOUT_CONFIG, "300000");
        producerProps.put(TRANSACTIONAL_ID_CONFIG, "prod-0");
        KafkaProducer<String, String> producer = new KafkaProducer(producerProps);
        ProducerRecord<String, String> record = new ProducerRecord<>("something", "A message");
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(record);
        producer.commitTransaction();
    }

    @Test
    public void test() {

    }
}
