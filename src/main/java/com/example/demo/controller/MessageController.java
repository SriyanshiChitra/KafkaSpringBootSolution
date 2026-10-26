package com.example.demo.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.DataModel.EventMessage;
import com.example.demo.DataModel.KafkaResponse;
import com.example.demo.Producer.MessageProducer;
import com.example.demo.repository.MessageRepository;
import com.example.demo.repository.MessageRepository.ConsumedRow;

import jakarta.validation.Valid;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;

@RestController
@RequestMapping ("/api/messages")
public class MessageController {

    private final MessageProducer messageProducer;
    private final MessageRepository messageRepository;

    public MessageController(MessageProducer messageProducer, MessageRepository messageRepository) {
        this.messageProducer = messageProducer;
        this.messageRepository = messageRepository;
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
    };
 @GetMapping 
 public List<MessageRepository.ConsumedRow> listMessages( @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "20") int size) {
    
        return messageRepository.list(page, size);
    }

    @GetMapping ("/{id}")
    public ConsumedRow find(@PathVariable UUID id) {
      
        return messageRepository.find(id).orElseThrow(() -> new RuntimeException("Message with id " + id + " not found"));
    }
}
