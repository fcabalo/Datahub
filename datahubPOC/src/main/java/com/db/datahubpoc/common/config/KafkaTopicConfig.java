package com.db.datahubpoc.common.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicConfig.class);

    @Value(value="${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value="${kafka.topic.all}")
    private String topicNames;

    @Bean
    public KafkaAdmin kafkaAdmin(){
        log.info("Creating Kafka admin client: bootstrapServers={}", bootstrapAddress);

        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

        return new KafkaAdmin(config);
    }

    @Bean
    public List<NewTopic> createKafkaTopics() {
        log.info("Initializing Kafka topics from configuration");

        List<NewTopic> topics = new ArrayList<>();
        if (topicNames != null && !topicNames.trim().isEmpty()) {
            String[] names = topicNames.split(",");
            for (String name : names) {
                String trimmedName = name.trim();
                if (!trimmedName.isEmpty()) {
                    topics.add(new NewTopic(trimmedName, 1, (short)1));
                    log.debug("Configured topic: name={}, partitions=1, replicationFactor=1", trimmedName);
                }
            }
        }

        log.info("Created {} topic configurations: {}", topics.size(),
                topics.stream().map(NewTopic::name).toList());

        return topics;
    }
}
