package com.ketul.producer.config;

import com.github.javafaker.Faker;
import com.ketul.model.Student;
import com.ketul.serailization.CustomSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class StudentProducer {

    private static Logger logger = LoggerFactory.getLogger(StudentProducer.class);

    public static void produceStudent() {
        KafkaProducer<String, Student> kafkaProducer = getKafkaProducer();
        for(int i = 1; i <= 10; i++) {
            Student student = getStudent();

            ProducerRecord<String, Student> studentRecord = new ProducerRecord<>("student", student);
            kafkaProducer.send(studentRecord);
        }
        logger.info("Event published...");
        kafkaProducer.flush();
        kafkaProducer.close();
        logger.info("Producer closed...");
//        Thread.sleep(10000);
    }

    private static Student getStudent() {
        Faker faker = new Faker();

        Student student = new Student();
        student.setName(faker.name().fullName());
        student.setMobileNo(faker.phoneNumber().phoneNumber());
        student.setEmail("abc@gmail.com");
        return student;
    }

    private static Properties getKafkaProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "student-id-producer");
        return props;
    }

    private static KafkaProducer<String, Student> getKafkaProducer(){
        return new KafkaProducer<>(getKafkaProducerConfig());
    }
}
