package com.example.demo.Producer;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.example.demo.DataModel.EventMessage;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;


@Service
public class MessageProducer {
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public MessageProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<String, String>> publish(EventMessage message) {
        try {
            String jsonMessage = objectMapper.writeValueAsString(message);
            return kafkaTemplate.send("messages", message.id().toString(), jsonMessage);
        } catch (JacksonException e) {
            throw new RuntimeException("Failed to serialize message", e);
        }
    }
}
