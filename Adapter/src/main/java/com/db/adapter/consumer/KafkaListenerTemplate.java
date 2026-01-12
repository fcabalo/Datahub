package com.db.adapter.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.integration.ip.IpHeaders;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

public class KafkaListenerTemplate implements MessageListener {

    private final MessageChannel toTcp;

    private String connectionId = new String();
    private final XmlMapper xmlMapper = new XmlMapper();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaListenerTemplate(String connectionId, MessageChannel toTcp){
        this.connectionId = connectionId;
        this.toTcp = toTcp;
    }

    @Override
    public void onMessage(Object data) {
        System.out.println("RECORD PROCESSING: " + data);
    }

    public void onMessage(ConsumerRecord<String, String> record) throws JsonProcessingException {
        JsonNode jsonNode = objectMapper.readTree(record.value());
        String xmlMessage = xmlMapper.writeValueAsString(jsonNode).replace("ObjectNode", "DatahubMessage");

        if (connectionId.isEmpty()) {
            System.out.println("No TCP client connected yet. Connect one and try again.");
        }

        Message<String> msg = MessageBuilder.withPayload(addLength(xmlMessage)).
                setHeader(IpHeaders.CONNECTION_ID, connectionId).
                build();
        toTcp.send(msg);
        System.out.println("Message Received and Sent: " + xmlMessage);
        System.out.println(record.value());
    }

    private static String addLength(String xmlMessage){
        return String.format("%04d%s", xmlMessage.length(), xmlMessage);
    }
}
