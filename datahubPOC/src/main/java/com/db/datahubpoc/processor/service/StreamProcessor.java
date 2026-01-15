package com.db.datahubpoc.processor.service;

import com.db.datahubpoc.common.entity.DatahubMessage;
import com.db.datahubpoc.integration.PartnerInterface;
import com.db.datahubpoc.integration.RoutingCriteria;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
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

@Component
public class StreamProcessor {
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Map<String, PartnerInterface> partnerInterfaces;

    @Autowired
    private List<RoutingCriteria> routingCriteria;

    @Value(value="${kafka.topic.incoming}")
    private String incomingTopic;

    @Value(value="${kafka.topic.deadletter}")
    private String deadLetter;

    @Autowired
    @DependsOn("createKafkaTopics")
    void buildPipeline(StreamsBuilder builder){

        KStream<String, String> messageStream = builder.stream(incomingTopic,
                Consumed.with(STRING_SERDE, STRING_SERDE));

        XmlMapper xmlMapper = new XmlMapper();
        ObjectMapper objectMapper = new ObjectMapper();

        messageStream.mapValues(
                value -> {
                    try{
                        if(value.startsWith("<")){
                            DatahubMessage message = xmlMapper.readValue(value, DatahubMessage.class);
                            updateMessage(message);
                            System.out.println(message);
                            return message;
                        }else{
                            return value;
                        }
                    } catch (JsonProcessingException e){
                        throw new RuntimeException(e);
                    }
                })
                .foreach((key, value) -> {
                    getOutgoingTopics((DatahubMessage) value)
                            .forEach(topic -> {
                                        System.out.println("Sending to " + topic + " - " + value);
                                        kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(value));
                                    });
                });
    }

    private List<String> getOutgoingTopics(DatahubMessage message){
        List<String> outgoingTopics = routingCriteria.stream()
                .filter(rc -> rc.getPartnerId() == null
                        || (message.getHeader().getDestination() != null && message.getHeader().getDestination().equals(rc.getPartnerId()) ))
                .filter(rc -> rc.getRecipientRegion() == null
                        || message.getHeader().getRegion().equals(rc.getRecipientRegion()))
                .filter(rc -> rc.getFormatType() == null
                        || message.getHeader().getFormatType().equals(rc.getFormatType()))
                .map(RoutingCriteria::getPartnerInterfaceId)
                .map(pi -> partnerInterfaces.get(pi))
                .filter(PartnerInterface::getLive)
                .map(PartnerInterface::getOutgoingTopic)
                .collect(Collectors.toList());
        if(outgoingTopics.isEmpty()){
            outgoingTopics.add(deadLetter);
        }

        return outgoingTopics;
    }

    private void updateMessage(DatahubMessage message){
        String region = partnerInterfaces.get(message.getHeader().getSource()).getRegion();
        message.getHeader().setRegion(region);
    }

}
