package com.example.demo.config;

import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import org.springframework.stereotype.Component;

@Component 
public class KafkaHealthIndicator  implements HealthIndicator {

    private final String  bootStrapServers;
    private final String topic;

    public KafkaHealthIndicator(@Value("${spring.kafka.bootstrap-servers}") String bootStrapServers, @Value("${app.kafka.topic.name}") String topic) {
        this.bootStrapServers = bootStrapServers;
        this.topic = topic;
    }
    @Override
    public Health health() {
        try{
            var admin=AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers));
            var topics=admin.listTopics().names().get(5, TimeUnit.SECONDS);
            boolean topicExists=topics.contains(topic);
            return topicExists ? Health.up().withDetail("topicsAccessible", topics.size()).build() : Health.down().withDetail("missingTopics", topics.size()).build();
         } catch (Exception e) {
            return Health.down(e).build();
        }
    }

    

}
