package com.example.demo.Consumer;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.example.demo.model.EventMessage;
import com.example.demo.repository.MessageRepository;

import tools.jackson.databind.ObjectMapper;

@Component 
@ComponentScan 
public class KafkaConsumer {

    private final ObjectMapper objectMapper;
    private final MessageRepository messageRepository;

    public KafkaConsumer(ObjectMapper objectMapper, MessageRepository messageRepository) {
        this.objectMapper = objectMapper    ;
        this.messageRepository = messageRepository;
    }

@KafkaListener (topics = "messages", groupId = "batch-service-v1")
    public void consume(List<ConsumerRecord<String, String>> message, Acknowledgment acknowledgment) {
        System.out.println("Consumed message: " + message);

        for(var record: message) {
            try{
                EventMessage eventMessage = objectMapper.readValue(record.value(), EventMessage.class);
                int inserted= messageRepository.insert(eventMessage, record.topic(), record.partition(), record.offset());
                System.out.println("Inserted message with id: " + eventMessage.id() + " into consumed_messages table. Rows affected: " + inserted);
            } catch (Exception e) {
                System.err.println("Error occurred while processing message with key: " + record.key());
                e.printStackTrace();
            }
            System.out.println("Consumed message with key: " + record.key() + ", value: " + record.value() + ", partition: " + record.partition() + ", offset: " + record.offset());
        }
        acknowledgment.acknowledge();
    }

}
