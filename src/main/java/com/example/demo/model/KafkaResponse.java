package com.example.demo.model;

import java.util.UUID;

public record KafkaResponse(
    UUID id,
    String topic,
    int partition,
    long offset
) {
}
