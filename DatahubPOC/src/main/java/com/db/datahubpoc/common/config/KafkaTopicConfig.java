package com.db.datahubpoc.common.config;

import com.db.datahubpoc.integration.Partner;
import com.db.datahubpoc.integration.PartnerInterface;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.config.TopicBuilder;
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

    @Value(value="${kafka.topic.incoming}")
    private String incomingTopic;


    @Bean
    public KafkaAdmin kafkaAdmin() {
        log.info("Creating Kafka admin client: bootstrapServers={}", bootstrapAddress);

        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

        return new KafkaAdmin(config);
    }

    @Bean
    @DependsOn("partnerInterfaces")
    public KafkaAdmin.NewTopics createKafkaTopics(Map<Integer, PartnerInterface> partnerInterfaces) {
        log.info("Creating Kafka topics for {} partner interfaces", partnerInterfaces.size());

        List<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(incomingTopic, 1, (short)1));
        log.info("Topic created: {}", incomingTopic);
        partnerInterfaces
                .forEach((p, v) ->{
                            String topicName = v.getTopicName();
                            topics.add(TopicBuilder.name(topicName).partitions(1).replicas(1).build());
                            log.info("Topic: {} created", topicName);
                        }
                );

        log.info("Kafka topic creation complete: {} topics created", topics.size());
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }
}
