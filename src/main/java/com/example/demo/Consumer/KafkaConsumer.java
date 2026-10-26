package com.example.demo.Consumer;

import java.util.List;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.example.demo.DataModel.EventMessage;
import com.example.demo.repository.MessageRepository;

import tools.jackson.databind.ObjectMapper;

@Component
public class KafkaConsumer {

    private final ObjectMapper objectMapper;
    private final MessageRepository messageRepository;

    public KafkaConsumer(ObjectMapper objectMapper, MessageRepository messageRepository) {
        this.objectMapper = objectMapper;
        this.messageRepository = messageRepository;
    }

    @KafkaListener(topics = "messages", groupId = "batch-service-v1")
    public void consume(List<ConsumerRecord<String, String>> message, Acknowledgment acknowledgment) {
        for (int i = 0; i < message.size(); i++) {
            var record = message.get(i);
            try {
                EventMessage eventMessage = objectMapper.readValue(record.value(), EventMessage.class);
                int inserted = messageRepository.insert(eventMessage, record.topic(), record.partition(), record.offset());
                if (inserted == 0) {
                    var existing = messageRepository.find(eventMessage.id()).orElseThrow();
                    boolean same = Objects.equals(existing.type(), eventMessage.type())
                            && Objects.equals(existing.description(), eventMessage.description())
                            && Objects.equals(existing.payload(), eventMessage.payload())
                            && Objects.equals(existing.createdBy(), eventMessage.createdBy())
                            && Objects.equals(existing.occurredAt(), eventMessage.createdAt());
                    if (!same) {
                        throw new IllegalArgumentException(
                                "Message with id: " + eventMessage.id() + " has different content than existing record. Skipping insertion.");
                    }
                }
                System.out.println("Consumed message with key: " + record.key() + ", value: " + record.value()
                        + ", partition: " + record.partition() + ", offset: " + record.offset());
            } catch (Exception e) {
                throw new BatchListenerFailedException(
                        "Error occurred while processing message with key: " + record.key(), e, i);
            }
        }

        acknowledgment.acknowledge();
    }
}
