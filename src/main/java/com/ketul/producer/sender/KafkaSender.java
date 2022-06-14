package com.ketul.producer.sender;

import com.ketul.model.Student;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class KafkaSender {

    private final Logger logger = LoggerFactory.getLogger(KafkaSender.class);

    @Autowired
    private KafkaTemplate<String, Student> userKafkaTemplate;

    public ListenableFuture<SendResult<String, Student>> sendCustomMessage(Student student, String topicName) {
        logger.info("Sending Json Serializer : {}", student);
        logger.info("--------------------------------");

        return userKafkaTemplate.send(topicName, student);
    }
}
