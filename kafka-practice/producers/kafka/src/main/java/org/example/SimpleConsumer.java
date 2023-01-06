package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SimpleConsumer {
    public static final Logger logger = LoggerFactory.getLogger(ConsumerWakeUp.class.getName());

    public static void main(String[] args) {
        String topicName = "pizza topic";
//        KafkaProducer configuration setting
        // null, "hello world"
        Properties props = new Properties();
//        bootstrap.servers,
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "simple-group2");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // heartbeat
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000");
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "90000");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");

        // static group membership
        props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1");
        // KafkaProducer object creation
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        // ProducerRecord object creation
    }
}

