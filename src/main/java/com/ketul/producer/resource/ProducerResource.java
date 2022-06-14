package com.ketul.producer.resource;

import com.ketul.model.Student;
import com.ketul.producer.sender.KafkaSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerResource {

    @Autowired
    private KafkaSender kafkaSender;

    @PostMapping("/send-student-info")
    public ResponseEntity<Object> sendMessage(@RequestBody Student student) {
        return ResponseEntity.ok(kafkaSender.sendCustomMessage(student, "student"));
    }
}
