package com.eventprocessing.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.converter.JsonMessageConverter;

@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.order-events}")
    private String orderEventsTopic;

    @Value("${app.kafka.topics.payment-events}")
    private String paymentEventsTopic;

    @Value("${app.kafka.topics.notification-events}")
    private String notificationEventsTopic;

    @Value("${app.kafka.topics.dead-letter}")
    private String deadLetterTopic;

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(orderEventsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentEventsTopic() {
        return TopicBuilder.name(paymentEventsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationEventsTopic() {
        return TopicBuilder.name(notificationEventsTopic)
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(deadLetterTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public JsonMessageConverter jsonMessageConverter() {
        return new JsonMessageConverter();
    }
}
