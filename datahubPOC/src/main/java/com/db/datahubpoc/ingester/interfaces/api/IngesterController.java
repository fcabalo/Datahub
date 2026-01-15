package com.db.datahubpoc.ingester.interfaces.api;

import com.db.datahubpoc.common.entity.DatahubMessage;
import com.db.datahubpoc.integration.PartnerInterface;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Log4j2
@RestController
@RequestMapping("datahub")
public class IngesterController {
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
        return datahubMessage;
    }

    @PostMapping(path="/")
    public String postMessage(@RequestBody String message){
        kafkaTemplate.send(incomingTopic, message);
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
        return ResponseEntity.ok().body("<h1>Welcome Datahub POC</h1>");
    }
}
