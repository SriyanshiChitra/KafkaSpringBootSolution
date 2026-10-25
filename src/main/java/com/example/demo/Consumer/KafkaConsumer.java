package com.example.demo.Consumer;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component 
@ComponentScan 
public class KafkaConsumer {
@KafkaListener (topics = "messages", groupId = "batch-service-v1")
    public void consume(List<ConsumerRecord<String, String>> message) {
        System.out.println("Consumed message: " + message);
    }
}
