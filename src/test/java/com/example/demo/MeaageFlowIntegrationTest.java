
package com.example.demo;

import com.example.demo.DataModel.EventMessage;
import com.example.demo.repository.MessageRepository;
import com.example.demo.Producer.MessageProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@Testcontainers
class MeaageFlowIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka:latest"));

    @Autowired
    MessageProducer producer;

    @Autowired
    MessageRepository repository;

    @Test
    void publishConsumesAndPersists() throws Exception {
        var msg = new EventMessage(
                UUID.randomUUID(), "IT", "desc", "payload", "tester", Instant.now());

        producer.publish(msg).get(10, TimeUnit.SECONDS);

        for (int i = 0; i < 60; i++) {
            if (repository.find(msg.id()).isPresent()) {
                return;
            }
            Thread.sleep(500);
        }

        fail("message was not consumed within 30s");
    }
}
