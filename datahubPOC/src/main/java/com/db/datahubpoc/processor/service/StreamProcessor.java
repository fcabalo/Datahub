package com.db.datahubpoc.processor.service;

import com.db.datahubpoc.common.entity.DatahubMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

@Component
public class StreamProcessor {
    private static final Serde<String> STRING_SERDE = Serdes.String();


    @Value(value="${kafka.topic.incoming}")
    private String incomingTopic;

    @Value(value="${kafka.topic.outgoing}")
    private String outgoingTopic;

    @Value("#{'${kafka.topic.incoming.all}'.split(',')}")
    private List<String> incomingTopics;

    @Value(value="${kafka.topic.outgoing1}")
    private String outgoing1;

    @Value(value="${kafka.topic.outgoing2}")
    private String outgoing2;

    @Autowired
    void buildPipeline(StreamsBuilder builder){
        /*KStream<String, String> messageStream = builder.stream(incomingTopic,
                Consumed.with(STRING_SERDE, STRING_SERDE));

         */

        KStream<String, String> messageStream = builder.stream(incomingTopics,
                Consumed.with(STRING_SERDE, STRING_SERDE));

        XmlMapper xmlMapper = new XmlMapper();
        ObjectMapper objectMapper = new ObjectMapper();

        messageStream.mapValues(
                value -> {
                    try{
                        if(value.startsWith("<")){
                            return objectMapper.writeValueAsString(xmlMapper.readValue(value, DatahubMessage.class));
                        }else{
                            return value;
                        }
                    } catch (JsonProcessingException e){
                        throw new RuntimeException(e);
                    }
                }).split()
                .branch((key, value) -> "A".equals(objectMapper.readValue(value, DatahubMessage.class).getFormatType()),
                        Branched.withConsumer((ks) -> ks.to(outgoing1)) )
                .branch((key, value) -> "B".equals(objectMapper.readValue(value, DatahubMessage.class).getFormatType()),
                        Branched.withConsumer((ks) -> ks.to(outgoing2)) )
                .noDefaultBranch();
    }
}
