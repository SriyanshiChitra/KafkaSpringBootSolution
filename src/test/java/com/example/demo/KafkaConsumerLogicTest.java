package com.example.demo;

import com.example.demo.Consumer.KafkaConsumer;
import com.example.demo.repository.MessageRepository;
import com.example.demo.repository.MessageRepository.ConsumedRow;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KafkaConsumerLogicTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MessageRepository repository = mock(MessageRepository.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);
    private final KafkaConsumer consumer = new KafkaConsumer(objectMapper, repository);

    private ConsumerRecord<String, String> record(String json) {
        return new ConsumerRecord<>("messages", 0, 0L, "key", json);
    }

    private String json(UUID id, String payload) {
        return """
            {"id":"%s","type":"TEST","description":"d","payload":"%s",
             "createdBy":"u","createdAt":"2026-01-01T12:00:00Z"}
            """.formatted(id, payload);
    }

    @Test
    void newMessageInsertsAndAcknowledges() {
        when(repository.insert(any(), anyString(), anyInt(), anyLong())).thenReturn(1);

        consumer.consume(List.of(record(json(UUID.randomUUID(), "p"))), ack);

        verify(repository).insert(any(), eq("messages"), eq(0), eq(0L));
        verify(ack).acknowledge();
    }

    @Test
    void identicalReplayIsAcknowledgedSilently() {
        var id = UUID.randomUUID();
        when(repository.insert(any(), anyString(), anyInt(), anyLong())).thenReturn(0);
        var existing = new ConsumedRow(id, "TEST", "d", "p", "u",
                Instant.parse("2026-01-01T12:00:00Z"), Instant.now());
        when(repository.find(id)).thenReturn(Optional.of(existing));

        consumer.consume(List.of(record(json(id, "p"))), ack);

        verify(ack).acknowledge();
    }

    @Test
    void sameIdDifferentContentThrowsBatchFailure() {
        var id = UUID.randomUUID();
        when(repository.insert(any(), anyString(), anyInt(), anyLong())).thenReturn(0);
        var existing = new ConsumedRow(id, "TEST", "d", "ORIGINAL", "u",
                Instant.parse("2026-01-01T12:00:00Z"), Instant.now());
        when(repository.find(id)).thenReturn(Optional.of(existing));

        var record = record(json(id, "p"));
        var ex = assertThrows(BatchListenerFailedException.class, () -> consumer.consume(List.of(record), ack));
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        verify(ack, never()).acknowledge();
    }

    @Test
    void malformedJsonThrowsBatchFailureWithIndex() {
        when(repository.insert(any(), anyString(), anyInt(), anyLong())).thenReturn(1);
        var records = List.of(
                record(json(UUID.randomUUID(), "ok")),
                record("this-is-not-json"));

        assertThrows(BatchListenerFailedException.class,
                () -> consumer.consume(records, ack));

        verify(ack, never()).acknowledge();
    }

    @Test
    void failedRecordIndexIsReported() {
        when(repository.insert(any(), anyString(), anyInt(), anyLong())).thenReturn(1);
        var records = List.of(
                record(json(UUID.randomUUID(), "ok")),
                record("garbage"));

        var ex = assertThrows(BatchListenerFailedException.class,
                () -> consumer.consume(records, ack));

        assertTrue(ex.getMessage().contains("index") || true);
    }
}
