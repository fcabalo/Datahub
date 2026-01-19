package com.db.adapter.common.config;

import com.db.adapter.interfaces.ConnectionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.GenericTransformer;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.StandardIntegrationFlow;
import org.springframework.integration.dsl.Transformers;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.ip.IpHeaders;
import org.springframework.integration.ip.dsl.Tcp;
import org.springframework.integration.ip.tcp.connection.*;
import org.springframework.integration.ip.tcp.serializer.ByteArrayCrLfSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import java.nio.charset.StandardCharsets;

@Configuration
public class TcpServerConfig {

    private static final Logger log = LoggerFactory.getLogger(TcpServerConfig.class);

    @Value(value="${tcp.server.port}")
    private int port;

    @Bean
    public MessageChannel fromTcp(){
        return new DirectChannel();
    }

    @Bean
    public MessageChannel toTcp(){
        return new DirectChannel();
    }

    @Bean
    public MessageChannel disconnectChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel incomingFromTcp() { return new QueueChannel(); }

    @Bean
    public AbstractServerConnectionFactory serverCF(){
        log.info("Initializing TCP server connection factory on port {}", port);

        TcpNetServerConnectionFactory  cf = new TcpNetServerConnectionFactory (this.port);
        cf.setSerializer(new ByteArrayCrLfSerializer());    // send with CRLF framing
        cf.setDeserializer(new ByteArrayCrLfSerializer());  // receive CRLF framing
        cf.setSingleUse(false);

        log.debug("TCP server configured with CRLF serialization, singleUse=false");

        return cf;
    }

    @Bean
    public IntegrationFlow outboundFlow(AbstractServerConnectionFactory serverCF) {
        log.debug("Creating outbound TCP flow");

        return IntegrationFlow.from(toTcp())
                .handle(Tcp.outboundAdapter(serverCF))
                .get();
    }

    private static String getPartnerId(String message){
        String trimmed = message.trim();
        if (trimmed.startsWith("PARTNER_ID=")){
            return trimmed.substring(11);
        }
        return null;
    }

    @ServiceActivator(inputChannel = "disconnectChannel")
    public void closeConnectionByConnectionId(Message message){
        String connectionId = (String) message.getHeaders().get(IpHeaders.CONNECTION_ID);
        log.info("Disconnecting connections: {}", connectionId);
        serverCF().closeConnection(connectionId);
    }

    @Bean
    public IntegrationFlow inboundFlow(AbstractServerConnectionFactory serverFactory,GenericTransformer bytesToString, ConnectionRegistry registry) {
        log.debug("Creating inbound TCP flow");

        return IntegrationFlow.from(Tcp.inboundAdapter(serverFactory))
                .transform(bytesToString)
                .route(Message.class, msg -> {
                    String connectionId = (String) msg.getHeaders().get(IpHeaders.CONNECTION_ID);
                    boolean hasSignon = registry.hasSignon(connectionId);
                    log.info("Routing message for connectionId={}, hasSignon={}", connectionId, hasSignon);
                    if(hasSignon){
                        return "message";
                    }
                    String payload = (String) msg.getPayload();
                    String partnerId = getPartnerId(payload);

                    if(partnerId != null){
                        return registry.hasRegistered(partnerId) ? "duplicate" : "signon";
                    } else {
                        return "message";
                    }
                }, mapping -> mapping
                        .subFlowMapping("signon", flow -> flow
                                .transform(TcpServerConfig::getPartnerId)
                                .handle((payload, header) -> {
                                    String connectionId = (String) header.get(IpHeaders.CONNECTION_ID);
                                    String partnerId = (String) payload;
                                    registry.register(connectionId, partnerId);
                                    log.info("Sign-on successful: partnerId={}, connectionId={}", partnerId, connectionId);
                                    return "PARTNER_ID=" + partnerId + " SIGN-ON SUCCESSFUL";
                                })
                                .channel("toTcp")
                        )
                        .subFlowMapping("message", flow -> flow
                                .handle((payload, header) -> {
                                    String connectionId = (String) header.get(IpHeaders.CONNECTION_ID);
                                    String partnerId = (String) payload;
                                    log.debug("Echoing message from connectionId={}, payload={}", connectionId, payload);
                                    return "SERVER ECHO: " + partnerId;
                                })
                                .channel("toTcp"))
                        .subFlowMapping("duplicate", flow -> flow
                                .transform(TcpServerConfig::getPartnerId)
                                .handle((payload, header) -> {
                                    String connectionId = (String) header.get(IpHeaders.CONNECTION_ID);
                                    String partnerId = (String) payload;
                                    log.info("Duplicate registration: partnerId={}", partnerId);
                                    return "PARTNER " + partnerId + " already signed-in and connected";
                                })
                                .routeToRecipients(r -> r
                                        .applySequence(true)
                                        .ignoreSendFailures(true)
                                        .recipient("toTcp")
                                        .recipient("disconnectChannel")
                                ))
                ).get();
    }

    @Bean
    public GenericTransformer<byte[], String> bytesToString() {
        return bytes -> new String(bytes, StandardCharsets.UTF_8);
    }
}
