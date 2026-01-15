package com.db.adapter.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.stereotype.Service;

@Service
public class KafkaListenerControlService {

    private static final Logger log = LoggerFactory.getLogger(KafkaListenerControlService.class);

    @Value(value="${spring.kafka.consumer.group-id}")
    private String kafkaGroupId;

    @Value(value="${spring.kafka.consumer.listener-id}")
    private String kafkaListenerId;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    private KafkaListenerContainerFactory containerFactory;

    @Autowired
    private MessageChannel toTcp;

    private KafkaListenerEndpoint createKafkaListenerEndpoint(String partnerId, String connectionId) {
        log.info("Creating Kafka listener endpoint for partnerId={}, connectionId={}", partnerId, connectionId);

        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                createDefaultMethodKafkaListenerEndpoint(partnerId);
        kafkaListenerEndpoint.setBean(new KafkaListenerTemplate(connectionId, toTcp));
        try {
            kafkaListenerEndpoint.setMethod(KafkaListenerTemplate.class.getMethod("onMessage", ConsumerRecord.class));
        } catch (NoSuchMethodException e) {
            log.error("Failed to set listener method for partnerId={}: method 'onMessage' not found", partnerId, e);
            throw new RuntimeException("Attempt to call a non-existent method " + e);
        }

        log.info("Successfully created Kafka listener endpoint for partnerId={}", partnerId);
        return kafkaListenerEndpoint;
    }

    private MethodKafkaListenerEndpoint<String, String> createDefaultMethodKafkaListenerEndpoint(String partnerId) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint = new MethodKafkaListenerEndpoint<>();
        String listenerId = generateListenerId(partnerId);
        String topic = getOutgoingTopic(partnerId);

        kafkaListenerEndpoint.setId(listenerId);
        kafkaListenerEndpoint.setGroupId(kafkaGroupId);
        kafkaListenerEndpoint.setTopics(topic);
        kafkaListenerEndpoint.setAutoStartup(false);
        kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());

        log.info("Configured default endpoint: listenerId={}, groupId={}, topic={}", listenerId, kafkaGroupId, topic);

        return kafkaListenerEndpoint;
    }

    public void createAndRegisterListener(String partnerId, String connectionId) {
        log.info("Registering new Kafka listener for partnerId={}, connectionId={}", partnerId, connectionId);

        KafkaListenerEndpoint listener = createKafkaListenerEndpoint(partnerId, connectionId);
        endpointRegistry.registerListenerContainer(listener, containerFactory, true);

        log.info("Successfully registered Kafka listener for partnerId={}", partnerId);
    }

    public void startListener(String listenerId){
        log.info("Attempting to start listener: {}", listenerId);

        MessageListenerContainer listenerContainer = endpointRegistry.getListenerContainer(listenerId);

        if (listenerContainer == null) {
            log.warn("Cannot start listener: no container found for listenerId={}", listenerId);
            return;
        }

        if (listenerContainer.isRunning()) {
            log.debug("Listener already running: listenerId={}", listenerId);
            return;
        }

        listenerContainer.start();
        log.info("Successfully started listener: {}", listenerId);
    }

    public void stopListener(String listenerId){
        listenerId = kafkaListenerId + listenerId;

        log.info("Attempting to stop and unregister listener: {}", listenerId);

        MessageListenerContainer listenerContainer = endpointRegistry.getListenerContainer(listenerId);

        if (listenerContainer == null) {
            log.warn("Cannot stop listener: no container found for listenerId={}", listenerId);
            return;
        }

        if (listenerContainer.isRunning()) {
            listenerContainer.stop();
            log.debug("Stopped listener container: {}", listenerId);
        } else {
            log.warn("Listener was not running: {}", listenerId);
        }

        endpointRegistry.unregisterListenerContainer(listenerId);
        listenerContainer.destroy();

        log.info("Successfully stopped and destroyed listener: {}", listenerId);
    }

    public String generateListenerId(String partnerId){
        return kafkaListenerId + partnerId;
    }

    public String getOutgoingTopic(String partnerId) {
        return "Partner" + partnerId + "Outgoing";
    }


}
