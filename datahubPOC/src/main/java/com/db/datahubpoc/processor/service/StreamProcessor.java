package com.db.datahubpoc.processor.service;

import com.db.datahubpoc.common.entity.DatahubMessage;
import com.db.datahubpoc.integration.PartnerInterface;
import com.db.datahubpoc.integration.RoutingCriteria;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Log4j2
@Component
public class StreamProcessor {
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Map<Integer, PartnerInterface> partnerInterfaces;

    @Autowired
    private List<RoutingCriteria> routingCriteria;

    @Value(value="${kafka.topic.incoming}")
    private String incomingTopic;

    @Autowired
    @DependsOn("createKafkaTopics")
    void buildPipeline(StreamsBuilder builder){

        KStream<String, String> messageStream = builder.stream(incomingTopic,
                Consumed.with(STRING_SERDE, STRING_SERDE));

        XmlMapper xmlMapper = new XmlMapper();

        messageStream.mapValues(
                value -> {
                    try{
                        if(value.startsWith("<")){
                            DatahubMessage message = xmlMapper.readValue(value, DatahubMessage.class);
                            log.info("Processing message {} from topic {}", message, incomingTopic);
                            updateMessage(message);
                            log.info("Updating message {}", message);
                            return message;
                        }else{
                            return value;
                        }
                    } catch (JsonProcessingException e){
                        throw new RuntimeException(e);
                    }
                })
                .foreach((key, value) -> {
                    getOutgoingPartnerInterfaces((DatahubMessage) value)
                            .forEach(pi -> {
                                        String convertedMessage = convertMessage((DatahubMessage) value,pi);
                                        kafkaTemplate.send(pi.getTopicName(), key, convertedMessage);
                                        log.info("Sending to topic [{}] message {}", pi.getTopicName(), convertedMessage);
                                    });
                });
    }

    private List<PartnerInterface> getOutgoingPartnerInterfaces(DatahubMessage message){
        List<PartnerInterface> outgoingPartners = routingCriteria.stream()
                .filter(rc -> rc.getPartnerId() == null
                        || (message.getHeader().getDestination() != null  && rc.getPartnerId().equals(message.getHeader().getDestination())))
                .filter(rc -> {
                    return switch(rc.getRecipientRegionOp()){
                        case null -> true;
                        case EQUALS -> rc.getRecipientRegion().equals(message.getHeader().getRegion());
                        case NOT_EQUALS -> !rc.getRecipientRegion().equals(message.getHeader().getRegion());
                        case IN -> rc.getRecipientRegion().contains(message.getHeader().getRegion());
                        case NOT_IN -> !rc.getRecipientRegion().contains(message.getHeader().getRegion());
                    };
                })
                .filter(rc -> {
                            return switch(rc.getMessageTypeOp()){
                                case null -> true;
                                case EQUALS -> rc.getMessageType().equals(message.getHeader().getMessageType());
                                case NOT_EQUALS -> !rc.getMessageType().equals(message.getHeader().getMessageType());
                                case IN -> rc.getMessageType().contains(message.getHeader().getMessageType());
                                case NOT_IN -> !rc.getMessageType().contains(message.getHeader().getMessageType());
                            };
                        }
                )
                .map(RoutingCriteria::getPartnerInterfaceId)
                .map(pi -> partnerInterfaces.get(pi))
                .collect(Collectors.toList());
        if(outgoingPartners.isEmpty()){
            // Add dead-letter topic as default
            outgoingPartners.add(partnerInterfaces.get(1));
        }

        return outgoingPartners;
    }

    /*
     * To add all default values
     */
    private void updateMessage(DatahubMessage message){
        String region = partnerInterfaces.get(message.getHeader().getSource()).getRegion();
        message.getHeader().setRegion(region);
    }

    /*
     * Convert messages into outgoing partners expected format
     */
    private String convertMessage(DatahubMessage message, PartnerInterface pi){
        String convertedMessage;
        ObjectMapper objectMapper = new ObjectMapper();

        switch (pi.getFormatType()){
            case null -> convertedMessage = objectMapper.writeValueAsString(message);
            case "UIC" -> convertedMessage = objectMapper.writeValueAsString(message);
            case "TAF/TAP" -> convertedMessage = message.toJsonString();
            default -> convertedMessage = objectMapper.writeValueAsString(message);
        }

        return convertedMessage;
    }
}
