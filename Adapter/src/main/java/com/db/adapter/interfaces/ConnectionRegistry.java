package com.db.adapter.interfaces;

import com.db.adapter.consumer.KafkaListenerControlService;
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

    private final ConcurrentMap<String, String> clientConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> connectedClients = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> partnerClient = new ConcurrentHashMap<>();

    @Autowired
    KafkaListenerControlService kafkaListenerControlService;

    @EventListener
    public void onOpen(TcpConnectionOpenEvent event) {
        System.out.println("Client connecting: " + event.getConnectionId());
    }

    public boolean hasSignon(String connectionId){
        System.out.println("Checking signon: " + connectionId);
        return clientConnections.containsKey(connectionId);
    }

    @EventListener
    public void onClose(TcpConnectionCloseEvent event) {
        String connectionId = event.getConnectionId();
        String partnerId = clientConnections.get(connectionId);
        clientConnections.remove(connectionId);
        connectedClients.remove(connectionId);
        partnerClient.remove(partnerId);

        System.out.println("Client disconnected: " + partnerId + ": " + connectionId);
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
        System.out.println("Partner: " + partnerId + " -- connection:" + connectionId + " Registered");
    }

}
