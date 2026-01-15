package com.db.adapter.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                createDefaultMethodKafkaListenerEndpoint(partnerId);
        kafkaListenerEndpoint.setBean(new KafkaListenerTemplate(connectionId, toTcp));
        try {
            kafkaListenerEndpoint.setMethod(KafkaListenerTemplate.class.getMethod("onMessage", ConsumerRecord.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Attempt to call a non-existent method " + e);
        }
        return kafkaListenerEndpoint;
    }

    private MethodKafkaListenerEndpoint<String, String> createDefaultMethodKafkaListenerEndpoint(String partnerId) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint = new MethodKafkaListenerEndpoint<>();
        kafkaListenerEndpoint.setId(generateListenerId(partnerId));
        kafkaListenerEndpoint.setGroupId(kafkaGroupId);
        kafkaListenerEndpoint.setTopics(getOutgoingTopic(partnerId));
        kafkaListenerEndpoint.setAutoStartup(false);
        kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());
        return kafkaListenerEndpoint;
    }

    public void createAndRegisterListener(String partnerId, String connectionId) {
        KafkaListenerEndpoint listener = createKafkaListenerEndpoint(partnerId, connectionId);
        endpointRegistry.registerListenerContainer(listener, containerFactory, true);
    }

    public void startListener(String listenerId){
        MessageListenerContainer listenerContainer = endpointRegistry.getListenerContainer(listenerId);
        if(listenerContainer != null && !listenerContainer.isRunning()){
            listenerContainer.start();
        }
    }

    public void stopListener(String listenerId){
        listenerId = kafkaListenerId + listenerId;
        MessageListenerContainer listenerContainer = endpointRegistry.getListenerContainer(listenerId);
        if(listenerContainer != null) {
            if (listenerContainer.isRunning()) {
                listenerContainer.stop();
            }
            endpointRegistry.unregisterListenerContainer(listenerId);
            listenerContainer.destroy();
        }
    }

    public String generateListenerId(String partnerId){
        return kafkaListenerId + partnerId;
    }

    public String getOutgoingTopic(String partnerId) {
        return "PI" + partnerId + "Outgoing";
    }


}
