package com.example.demo.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class KafkaProducerController {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	@GetMapping("/send")
	public void sendMessage(@RequestParam("msg") String msg) {
		
	      kafkaTemplate.send("TestTopic", msg);
	      System.out.println("published");
	   }
}
