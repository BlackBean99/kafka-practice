package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class FileProducer {
    public static final Logger logger = LoggerFactory.getLogger(FileProducer.class.getName());

    public static void main(String[] args) {
        String topicName = "multipart-topic";
//        KafkaProducer configuration setting
        // null, "hello world"
        Properties props = new Properties();
//        bootstrap.servers,
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // KafkaProducer object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        String filePath = "/Users/blackbean/IdeaProjects/KafkaProj01/kafka-practice/practice/src/main/resources/pizza_sample.txt";        // KafkaProducer 객체 생성 -> ProducerRecords생성 -> send
        sendFileMessages(kafkaProducer, topicName, filePath);
        kafkaProducer.close();
    }

    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer, String topicName, String filePath) {
        String line = "";
        final String delimeter = ",";
        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferReader = new BufferedReader(fileReader);
            while ((line = bufferReader.readLine()) != null) {
                String[] tokens = line.split(delimeter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();
                for (int i = 0; i < tokens.length; i++) {
                    if(i != (tokens.length) -1) {
                        value.append(tokens[i] + ",");
                    } else {
                        value.append(tokens[i]);
                    }
                }
                sendMessage(kafkaProducer, topicName, key, value.toString());
            }
        }catch(Exception e){
            logger.info(e.getMessage());
        }
    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer, String topicName, String key, String value) {
        // ProducerRecord object creation
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);

        // kafkaProducer message send
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info("\n #### record metadata received ### \n" +
                        "partition: " + metadata.partition() +
                        "offset: " + metadata.offset() +
                        "timestamp: " + metadata.timestamp()
                );
            } else {
                logger.error("exception error from broker" + exception.getMessage());
            }
        });
    }
}
