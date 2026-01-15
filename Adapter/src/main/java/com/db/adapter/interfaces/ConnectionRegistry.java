package com.db.adapter.interfaces;

import com.db.adapter.consumer.KafkaListenerControlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.integration.ip.tcp.connection.TcpConnectionCloseEvent;
import org.springframework.integration.ip.tcp.connection.TcpConnectionOpenEvent;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class ConnectionRegistry {

    private static final Logger log = LoggerFactory.getLogger(ConnectionRegistry.class);

    private final ConcurrentMap<String, String> clientConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> connectedClients = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> partnerClient = new ConcurrentHashMap<>();

    @Autowired
    KafkaListenerControlService kafkaListenerControlService;

    @EventListener
    public void onOpen(TcpConnectionOpenEvent event) {
        log.info("Client connecting: {}", event.getConnectionId());
    }

    public boolean hasSignon(String connectionId){
        log.info("Checking sign-on: {}", connectionId);
        return clientConnections.containsKey(connectionId);
    }

    @EventListener
    public void onClose(TcpConnectionCloseEvent event) {
        String connectionId = event.getConnectionId();
        String partnerId = clientConnections.get(connectionId);
        clientConnections.remove(connectionId);
        connectedClients.remove(connectionId);
        partnerClient.remove(partnerId);

        log.info("Client disconnected: {}: {}", partnerId, connectionId);
        kafkaListenerControlService.stopListener(partnerId);
    }

    public Optional<String> getConnectionId(String partnerId) {
        return Optional.ofNullable(partnerClient.get(partnerId));
    }

    public void register(String connectionId, String partnerId){
        clientConnections.put(connectionId, partnerId);
        connectedClients.put(connectionId, Boolean.TRUE);
        partnerClient.put(partnerId, connectionId);
        kafkaListenerControlService.createAndRegisterListener(partnerId, connectionId);

        log.info("Partner: {} -- connection: {} Registered", partnerId, connectionId);
    }

}
