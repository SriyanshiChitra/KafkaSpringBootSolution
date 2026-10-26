package com.example.demo.DataModel;

import java.util.UUID;

public record KafkaResponse(
    UUID id,
    String topic,
    int partition,
    long offset
) {
}
