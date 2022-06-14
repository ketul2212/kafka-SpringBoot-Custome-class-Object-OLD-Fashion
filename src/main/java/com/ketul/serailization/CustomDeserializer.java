package com.ketul.serailization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ketul.model.Student;
import org.apache.commons.lang.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class CustomDeserializer implements Deserializer<Student>{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Student deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                System.out.println("Null received at deserializing");
                return null;
            }
            System.out.println("Deserializing...");
            return objectMapper.readValue(data, Student.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to Student");
        }
    }

    @Override
    public void close() {
    }
}
