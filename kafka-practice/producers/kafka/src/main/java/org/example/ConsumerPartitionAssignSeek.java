package org.example;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerPartitionAssignSeek {
    public static final Logger logger = LoggerFactory.getLogger(ConsumerPartitionAssignSeek.class.getName());

    public static void main(String[] args) {
        String topicName = "pizza-topic";
//        KafkaProducer configuration setting
        // null, "hello world"
        Properties props = new Properties();
//        bootstrap.servers,
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_pizza_assign_seek");
//         props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        // static group membership
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        kafkaConsumer.assign(Arrays.asList(topicPartition));
        kafkaConsumer.seek(topicPartition, 10L);
        // ProducerRecord object creation

        //mainThread 참조 변수
        Thread mainThread = Thread.currentThread();
        // main Thread 종료시 별도의 thread로 kafkaConsumer wakeup()메소드를 호출R
        kafkaConsumer.subscribe(List.of(topicName));
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                logger.info(" main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();
                try{
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        pollAutoCommit(kafkaConsumer);
        pollNoCommit(kafkaConsumer);
    }

    private static void pollNoCommit(KafkaConsumer<String, String> kafkaConsumer) {

        int loopCnt = 0;
        try{
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ##### loopCnt :{} consumerRecord count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{}, record value:{}, partition:{}, record offset:{}, record.value:{}",
                            record.key(), record.value(), record.partition(), record.offset(), record.value());
                }
            }
        }catch (WakeupException e){
            logger.error("wakeup exception has been called");
        }finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

    public static void pollAutoCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ##### loopCnt :{} consumerRecord count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{}, record value:{}, partition:{}, record offset:{}, record.value:{}",
                            record.key(), record.value(), record.partition(), record.offset(), record.value());
                }
                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                  @Override
                  public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                      if (exception != null) {
                          logger.error("offsets {} is not completed, error:{}", offsets, exception);
                      }
                  }
              }
                );
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

    public static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {

        int loopCnt = 0;
        try{
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ##### loopCnt :{} consumerRecord count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{}, record value:{}, partition:{}, record offset:{}, record.value:{}",
                            record.key(), record.value(), record.partition(), record.offset(), record.value());
                }
                try {
                    if(consumerRecords.count() > 0) {
                        kafkaConsumer.commitSync();
                        logger.info("commit sync success");
                    }
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        }catch (WakeupException e){
            logger.error("wakeup exception has been called");
        }finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }

    }
}
