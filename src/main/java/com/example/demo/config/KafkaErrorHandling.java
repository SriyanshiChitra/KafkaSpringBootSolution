package com.example.demo.config;

import java.beans.BeanProperty;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration 
public class KafkaErrorHandling {
    
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,(record,ex)->
    new TopicPartition("messages" + ".DLQ", record.partition()));
    recoverer.setFailIfSendResultIsError(true);
    var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
    handler.setCommitRecovered(true);
        return handler;
    }
}
