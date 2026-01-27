package com.db.datahubpoc.processor.service;

import com.db.datahubpoc.common.entity.DatahubMessage;
import com.db.datahubpoc.integration.PartnerInterface;
import com.db.datahubpoc.integration.RoutingCriteria;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.micrometer.core.annotation.Timed;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

@Component
public class StreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(StreamProcessor.class);

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Map<Integer, PartnerInterface> partnerInterfaces;

    @Autowired
    private List<RoutingCriteria> routingCriteria;

    @Autowired
    private MessageProcessingService messageProcessingService;

    @Value(value="${kafka.topic.incoming}")
    private String incomingTopic;

    @Autowired
    @Timed("stream.processor.time")
    @DependsOn("createKafkaTopics")
    void buildPipeline(StreamsBuilder builder){
        log.info("Building Kafka Streams pipeline: incomingTopic={}", incomingTopic);

        KStream<String, String> messageStream = builder.stream(incomingTopic,
                Consumed.with(STRING_SERDE, STRING_SERDE));

        XmlMapper xmlMapper = new XmlMapper();
        ObjectMapper objectMapper = new ObjectMapper();

        messageStream.mapValues(
                value -> {
                    try{
                        if(value.startsWith("<")){
                            log.debug("Converting XML message to JSON");

                            DatahubMessage message = xmlMapper.readValue(value, DatahubMessage.class);
                            log.info("Processing message {} from topic {}", message, incomingTopic);
                            return message;
                        }else{
                            log.debug("Message already in JSON format, passing through");
                            DatahubMessage message = objectMapper.readValue(value, DatahubMessage.class);
                            return message;
                        }
                    } catch (JsonProcessingException e){
                        log.error("Failed to process message: {}", e.getMessage(), e);

                        throw new RuntimeException(e);
                    }
                })
                .foreach((key, value) -> {
                    messageProcessingService.getOutgoingPartnerInterfaces((DatahubMessage) value)
                            .forEach(pi -> {
                                        String convertedMessage = messageProcessingService.convertMessage((DatahubMessage) value,pi);
                                        kafkaTemplate.send(pi.getTopicName(), key, convertedMessage);
                                        log.info("Sending to topic [{}] message {}", pi.getTopicName(), convertedMessage);
                                    });
                });
        log.info("Kafka Streams pipeline built successfully");
    }
}
