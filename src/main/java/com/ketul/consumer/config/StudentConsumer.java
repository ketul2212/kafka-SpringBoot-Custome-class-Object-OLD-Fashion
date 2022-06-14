package com.ketul.consumer.config;

import com.ketul.model.Student;
import com.ketul.serailization.CustomDeserializer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Configuration
public class StudentConsumer {

    private final static Logger logger = LoggerFactory.getLogger(StudentConsumer.class);

    private static final KafkaConsumer<String, Student> kafkaConsumer= getKafkaConsumer();
    public static void startConsumer() {
        kafkaConsumer.subscribe(Collections.singletonList("student"));
        ConsumerRecords<String, Student> studentRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

        logger.info("Consumed request count ---> {}", studentRecords.count());
        while(studentRecords.count() > 0) {
            try {
                System.out.println(studentRecords.count());
                studentRecords.forEach(stringStudentConsumerRecord -> {
                    System.out.println(stringStudentConsumerRecord.value());
                    kafkaConsumer.commitAsync();
                });
            } catch(NullPointerException npe) {
                npe.printStackTrace();
            }
            studentRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
        }
    }
    private static KafkaConsumer<String,Student> getKafkaConsumer(){
        return new KafkaConsumer<>(getKafkaConsumerConfig());
    }
    private static Properties getKafkaConsumerConfig(){
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, RandomStringUtils.randomAlphanumeric(7));
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, RandomStringUtils.randomAlphanumeric(7));
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
//        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.ketul");
//        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerProps;
    }

}
