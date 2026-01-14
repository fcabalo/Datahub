package com.db.datahubpoc.common.config;

import com.db.datahubpoc.integration.Partner;
import com.db.datahubpoc.integration.PartnerInterface;
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

@Configuration
public class KafkaTopicConfig {

    @Value(value="${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value="${kafka.topic.incoming}")
    private String incomingTopic;

    @Value(value="${kafka.topic.deadletter}")
    private String deadLetter;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

        return new KafkaAdmin(config);
    }

    @Bean
    @DependsOn("partnerInterfaces")
    public KafkaAdmin.NewTopics createKafkaTopics(Map<String, PartnerInterface> partnerInterfaces) {
        List<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(incomingTopic, 1, (short)1));
        System.out.println("Topic: "+ incomingTopic + " created");
        topics.add(new NewTopic(deadLetter, 1, (short)1));
        System.out.println("Topic: "+ deadLetter + " created");
        partnerInterfaces
                .forEach((p, v) ->{
                            if(v.getDirection().equals("incoming")){
                                topics.add(TopicBuilder.name("PI" + p + "Incoming").partitions(1).replicas(1).build());
                                System.out.println("Topic: "+ "PI" + p + "Incoming" + " created");
                            } else{
                                topics.add(TopicBuilder.name("PI" + p + "Outgoing").partitions(1).replicas(1).build());
                                System.out.println("Topic: "+ "PI" + p + "Outgoing" + " created");
                            }
                        }
                );
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }
}
