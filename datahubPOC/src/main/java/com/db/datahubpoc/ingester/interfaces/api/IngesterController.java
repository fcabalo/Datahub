package com.db.datahubpoc.ingester.interfaces.api;

import com.db.datahubpoc.common.entity.DatahubMessage;
import com.db.datahubpoc.integration.PartnerInterface;
import com.db.datahubpoc.monitoring.MessageProcessingMetricsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.micrometer.core.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/datahub")
public class IngesterController {

    private static final Logger log = LoggerFactory.getLogger(IngesterController.class);

    private static final AtomicLong currTime = new AtomicLong(System.currentTimeMillis());

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value(value="${kafka.topic.incoming}")
    private String incomingTopic;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Map<Integer, PartnerInterface> partnerInterfaces;

    private XmlMapper xmlMapper = new XmlMapper();

    @Autowired
    private MessageProcessingMetricsService messageProcessingMetricsService;

    @PostMapping(path="", consumes = MediaType.APPLICATION_XML_VALUE, produces = MediaType.APPLICATION_XML_VALUE)
    @Timed(value = "ingester.timer", description = "Time taken for postXMLMessage")
    public DatahubMessage postXMLMessage(@RequestBody DatahubMessage datahubMessage) throws JsonProcessingException {
        String message = xmlMapper.writeValueAsString(datahubMessage);
        log.info("Message received: {}", message);
        Integer source = datahubMessage.getHeader().getSource();
        String topic = partnerInterfaces.get(source).getTopicName();
        String key = generateKey(source);

        kafkaTemplate.send(topic, key, message);
        log.info("Message sent to topic {}", topic);

        updateMessage(datahubMessage);
        kafkaTemplate.send(incomingTopic, key, objectMapper.writeValueAsString(datahubMessage));
        log.info("Message sent to topic {}", incomingTopic);

        log.info("XML message processed successfully");
        messageProcessingMetricsService.incrementMessageType(datahubMessage.getHeader().getMessageType());
        return datahubMessage;
    }

    @PostMapping(path="")
    public String postMessage(@RequestBody String message){
        log.info("Received raw message, length={}", message.length());

        kafkaTemplate.send(incomingTopic, message);
        log.info("Raw message sent to topic={}", incomingTopic);
        log.trace("Message content: {}", message);

        return message;
    }

    @PostMapping(path="", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @Timed(value = "ingester.timer", description = "Time taken for postXMLMessage")
    public DatahubMessage postJSONMessage(@RequestBody DatahubMessage datahubMessage) throws JsonProcessingException {
        Integer source = datahubMessage.getHeader().getSource();
        String topic = partnerInterfaces.get(source).getTopicName();
        String key = generateKey(source);

        log.info("Message received: {}", datahubMessage);
        kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(datahubMessage));
        log.info("Message sent to topic {}", topic);
        updateMessage(datahubMessage);
        kafkaTemplate.send(incomingTopic, key, objectMapper.writeValueAsString(datahubMessage));
        log.info("Message sent to topic {}", incomingTopic);
        messageProcessingMetricsService.incrementMessageType(datahubMessage.getHeader().getMessageType());
        return datahubMessage;
    }

    @GetMapping("")
    @Timed(value = "test_timer", description = "Time taken for testing")
    public ResponseEntity<String> showWelcome(){
        log.debug("Welcome endpoint accessed");

        return ResponseEntity.ok().body("<h1>Welcome Datahub POC</h1>");
    }

    private void updateMessage(DatahubMessage message){
        String region = partnerInterfaces.get(message.getHeader().getSource()).getRegion();
        Date receivedDate = new Date();
        message.getHeader().setRegion(region);
        message.getHeader().setReceivedDate(receivedDate);
    }

    private String generateKey(Integer sourceId) {
        return String.format("%04d%s", sourceId, Long.toString(currTime.incrementAndGet()).substring(1));
    }
}
