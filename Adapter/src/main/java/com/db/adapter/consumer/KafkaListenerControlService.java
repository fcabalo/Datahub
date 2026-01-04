package com.db.adapter.consumer;

import com.db.adapter.common.config.KafkaListenerTemplate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
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

    private KafkaListenerEndpoint createKafkaListenerEndpoint(String partnerId) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                createDefaultMethodKafkaListenerEndpoint(topic);
        kafkaListenerEndpoint.setBean(new KafkaListenerTemplate());
        try {
            kafkaListenerEndpoint.setMethod(KafkaListenerTemplate.class.getMethod("onMessage", ConsumerRecord.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Attempt to call a non-existent method " + e);
        }
        return kafkaListenerEndpoint;
    }

    private MethodKafkaListenerEndpoint<String, String> createDefaultMethodKafkaListenerEndpoint(String partnerId) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint = new MethodKafkaListenerEndpoint<>();
        kafkaListenerEndpoint.setId(generateListenerId());
        kafkaListenerEndpoint.setGroupId(kafkaGroupId);
        kafkaListenerEndpoint.setAutoStartup(true);
        kafkaListenerEndpoint.setTopics(topic);
        kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());
        return kafkaListenerEndpoint;
    }

    public void createAndRegisterListener(String topic) {
        KafkaListenerEndpoint listener = createKafkaListenerEndpoint(topic);
        kafkaListenerEndpointRegistry.registerListenerContainer(listener, kafkaListenerContainerFactory, true);
    }

    public void startListener(String listenerId){
        MessageListenerContainer listenerContainer = registry.getListenerContainer(listenerId);
        if(listenerContainer != null && !listenerContainer.isRunning()){
            listenerContainer.start();
        }
    }

    public void stopListener(String listenerId){
        MessageListenerContainer listenerContainer = registry.getListenerContainer(listenerId);
        if(listenerContainer != null && listenerContainer.isRunning()){
            listenerContainer.stop();
        }
    }

    private String generateListenerId(String partnerId){
        return kafkaListenerId + partnerId;
    }

    private String
}
