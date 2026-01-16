package com.db.datahubpoc.ingester.interfaces.api;

import com.db.datahubpoc.common.entity.DatahubMessage;
import com.db.datahubpoc.integration.PartnerInterface;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("datahub")
public class IngesterController {

    private static final Logger log = LoggerFactory.getLogger(IngesterController.class);

    @Value(value="${kafka.topic.incoming}")
    private String incomingTopic;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Map<Integer, PartnerInterface> partnerInterfaces;

    private XmlMapper xmlMapper = new XmlMapper();

    @PostMapping(path="/", consumes = MediaType.APPLICATION_XML_VALUE, produces = MediaType.APPLICATION_XML_VALUE)
    public DatahubMessage postXMLMessage(@RequestBody DatahubMessage datahubMessage) throws JsonProcessingException {
                String message = xmlMapper.writeValueAsString(datahubMessage);
        log.info("Message received: {}", message);
        String topic = partnerInterfaces.get(datahubMessage.getHeader().getSource()).getTopicName();
        kafkaTemplate.send(topic, message);
        log.info("Message sent to topic {}", topic);
        kafkaTemplate.send(incomingTopic, message);
        log.info("Message sent to topic {}", incomingTopic);

        log.info("XML message processed successfully");

        return datahubMessage;
    }

    @PostMapping(path="/")
    public String postMessage(@RequestBody String message){
        log.info("Received raw message, length={}", message.length());

        kafkaTemplate.send(incomingTopic, message);
        log.info("Raw message sent to topic={}", incomingTopic);
        log.trace("Message content: {}", message);

        return message;
    }

    @PostMapping(path="/", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public DatahubMessage postJSONMessage(@RequestBody DatahubMessage datahubMessage){
        String topic = partnerInterfaces.get(datahubMessage.getHeader().getSource()).getTopicName();
        String message = datahubMessage.toJsonString();
        log.info("Message received: {}", message);
        kafkaTemplate.send(topic, message);
        log.info("Message sent to topic {}", topic);
        kafkaTemplate.send(incomingTopic, message);
        log.info("Message sent to topic {}", incomingTopic);
        return datahubMessage;
    }

    @GetMapping("/")
    public ResponseEntity<String> showWelcome(){
        log.debug("Welcome endpoint accessed");

        return ResponseEntity.ok().body("<h1>Welcome Datahub POC</h1>");
    }
}
