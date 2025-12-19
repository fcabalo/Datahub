package com.db.adapter.consumer;

import com.db.adapter.interfaces.ConnectionRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.ip.IpHeaders;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class OutgoingMessageConsumer {

    private final static XmlMapper xmlMapper = new XmlMapper();
    private final static ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private final MessageChannel toTcp;

    @Autowired
    private ConnectionRegistry connectionRegistry;

    public OutgoingMessageConsumer(MessageChannel toTcp, ConnectionRegistry connectionRegistry) {
        this.toTcp = toTcp;
        this.connectionRegistry = connectionRegistry;
    }

    @KafkaListener(topics = "#{'${kafka.topic.in.all}'.split(',')}", groupId = "${spring.kafka.consumer.group-id}")
    public void listenGroup(String message) throws JsonProcessingException {
        JsonNode jsonNode = objectMapper.readTree(message);
        String xmlMessage = xmlMapper.writeValueAsString(jsonNode).replace("ObjectNode", "Message");


        Optional<String> connectionId = connectionRegistry.currentClient();
        if (connectionId.isEmpty()) {
            System.out.println("No TCP client connected yet. Connect one and try again.");
        }

        Message<String> msg = MessageBuilder.withPayload(xmlMessage).
                setHeader(IpHeaders.CONNECTION_ID, connectionId.get()).
                build();
        toTcp.send(msg);
        System.out.println("Message Received and Sent: " + xmlMessage);
    }
}
