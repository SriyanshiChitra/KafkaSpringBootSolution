package com.example.demo.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.example.demo.model.EventMessage;
import com.example.demo.model.KafkaResponse;
import com.example.demo.service.MessageProducer;

import jakarta.validation.Valid;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;

@RestController
@RequestMapping ("/api/messages")
public class MessageProducerController {

    private final MessageProducer messageProducer;

    public MessageProducerController(MessageProducer messageProducer) {
        this.messageProducer = messageProducer;
    };

    @PostMapping
    public CompletableFuture<ResponseEntity<KafkaResponse>> publish(@Valid  @RequestBody EventMessage message) {
        return messageProducer.publish(message).thenApply((SendResult<String, String> result) -> {
            var metadata = result.getRecordMetadata();
            var kafkaResponse = new KafkaResponse(message.id(), metadata.topic(), metadata.partition(), metadata.offset());
            return ResponseEntity.accepted()
                                .location(URI.create("api/messages/"+message.id()))
                                .body(kafkaResponse);
        }).exceptionally( ex -> {
            System.err.println("Failed to send message: " + ex.getMessage());
            return null;
        });
    }
}
