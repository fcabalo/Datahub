package com.db.adapter.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.ip.IpHeaders;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

public class KafkaListenerTemplate implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaListenerTemplate.class);

    private final MessageChannel toTcp;

    private String connectionId = new String();
    private final XmlMapper xmlMapper = new XmlMapper();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaListenerTemplate(String connectionId, MessageChannel toTcp){
        this.connectionId = connectionId;
        this.toTcp = toTcp;

        log.info("Initialized KafkaListenerTemplate with connectionId={}", connectionId);
    }

    @Override
    public void onMessage(Object data) {
        log.debug("Received raw message: {}", data);
    }

    public void onMessage(ConsumerRecord<String, String> record) throws JsonProcessingException {
        log.debug("Processing record from topic={}, partition={}, offset={}",
                record.topic(), record.partition(), record.offset());

        JsonNode jsonNode = objectMapper.readTree(record.value());
        String xmlMessage = xmlMapper.writeValueAsString(jsonNode).replace("ObjectNode", "DatahubMessage");

        if (connectionId.isEmpty()) {
            log.warn("No TCP client connected. Message cannot be delivered. Topic={}, offset={}",
                    record.topic(), record.offset());
        }

        Message<String> msg = MessageBuilder.withPayload(addLength(xmlMessage)).
                setHeader(IpHeaders.CONNECTION_ID, connectionId).
                build();
        toTcp.send(msg);

        log.info("Message sent to TCP client. connectionId={}, messageLength={}",
                connectionId, xmlMessage.length());
        log.debug("Message content: {}", xmlMessage);
        log.trace("Original record value: {}", record.value());
    }

    private static String addLength(String xmlMessage){
        return String.format("%04d%s", xmlMessage.length(), xmlMessage);
    }
}
