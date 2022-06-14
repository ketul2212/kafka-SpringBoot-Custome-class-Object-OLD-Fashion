package com.ketul;

import com.ketul.consumer.config.StudentConsumer;
import com.ketul.producer.config.StudentProducer;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class KafkaDemoApplication {
	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplication.class, args);

		StudentProducer.produceStudent();
//		StudentConsumer.startConsumer();
	}

	@Scheduled(fixedDelayString = "PT5S")
	public static void scheduling() {
		System.out.println("Hello!");
		StudentConsumer.startConsumer();
	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name("student-data")
				.partitions(1)
				.replicas(1)
				.build();
	}
}
