package com.db.datahubpoc.common.config;

import com.db.datahubpoc.integration.Partner;
import com.db.datahubpoc.integration.PartnerInterface;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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

@Log4j2
@Configuration
public class KafkaTopicConfig {

    @Value(value="${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value="${kafka.topic.incoming}")
    private String incomingTopic;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(config);
    }

    @Bean
    @DependsOn("partnerInterfaces")
    public KafkaAdmin.NewTopics createKafkaTopics(Map<Integer, PartnerInterface> partnerInterfaces) {
        List<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(incomingTopic, 1, (short)1));
        log.info("Topic: {} created", incomingTopic);
        partnerInterfaces
                .forEach((p, v) ->{
                            String topicName = v.getTopicName();
                            topics.add(TopicBuilder.name(topicName).partitions(1).replicas(1).build());
                            log.info("Topic: {} created", topicName);
                        }
                );
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }
}
