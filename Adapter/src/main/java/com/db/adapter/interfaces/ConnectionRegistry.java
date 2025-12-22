package com.db.adapter.interfaces;

import com.db.adapter.consumer.KafkaListenerControlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.integration.ip.IpHeaders;
import org.springframework.integration.ip.tcp.connection.TcpConnectionCloseEvent;
import org.springframework.integration.ip.tcp.connection.TcpConnectionOpenEvent;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class ConnectionRegistry {

    private final AtomicReference<String> currentConnectionId = new AtomicReference<>();

    @Autowired
    KafkaListenerControlService kafkaListenerControlService;

    @Value(value="${spring.kafka.consumer.listener-id}")
    private String listenerId;

    @EventListener
    public void onOpen(TcpConnectionOpenEvent event) {
        currentConnectionId.set(event.getConnectionId());
        System.out.println("Client connected: " + event.getConnectionId());
        kafkaListenerControlService.startListener(listenerId);
    }

    @EventListener
    public void onClose(TcpConnectionCloseEvent event) {
        String id = event.getConnectionId();
        System.out.println("Client disconnected: " + id);
        currentConnectionId.compareAndSet(id, null);
        kafkaListenerControlService.stopListener(listenerId);
    }

    public Optional<String> currentClient() {
        return Optional.ofNullable(currentConnectionId.get());
    }

    public void printTargetHint(MessageHeaders headers) {
        Object id = headers.get(IpHeaders.CONNECTION_ID);
        System.out.println("Sent to connectionId=" + id);
    }

}
