package com.example.demo.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class SpringbootKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringbootKafkaApplication.class, args);
	}
	
	 @KafkaListener(topics = "#{'${kafka.topics}'}", groupId = "group-id")
	   public void listen(String message) {
	      System.out.println("Received Messasge in group - group-id: " + message);
	   }


}
