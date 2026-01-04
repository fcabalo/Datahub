package com.db.adapter.common.config;

import com.db.adapter.interfaces.ConnectionRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.GenericTransformer;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.ip.IpHeaders;
import org.springframework.integration.ip.dsl.Tcp;
import org.springframework.integration.ip.tcp.connection.*;
import org.springframework.integration.ip.tcp.serializer.ByteArrayCrLfSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import java.nio.charset.StandardCharsets;


@Configuration
public class TcpServerConfig {

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
    public MessageChannel incomingFromTcp() { return new DirectChannel(); }

    /*@Bean
    @ServiceActivator(inputChannel = "toTcp")
    public MessageHandler tcpOutGate(AbstractClientConnectionFactory connectionFactory){
        TcpOutboundGateway gate = new TcpOutboundGateway();
        gate.setConnectionFactory(connectionFactory);
        gate.setOutputChannelName("resultToString");
        return gate;
    }*/

    @Bean
    public AbstractServerConnectionFactory serverCF(){
        TcpNetServerConnectionFactory cf = new TcpNetServerConnectionFactory(this.port);
        cf.setSerializer(new ByteArrayCrLfSerializer());    // send with CRLF framing
        cf.setDeserializer(new ByteArrayCrLfSerializer());  // receive CRLF framing
        cf.setSingleUse(false);
        return cf;
    }

    @Bean
    public IntegrationFlow outboundFlow() {
        return IntegrationFlow.from(toTcp())
                .handle(Tcp.outboundAdapter(serverCF())) // uses IpHeaders.CONNECTION_ID to target a client
                .get();
    }

    private static String getPartnerId(String message){
        String trimmed = message.trim();
        if (trimmed.startsWith("PARTNER_ID=")){
            return trimmed.substring(11);
        }
        return null;
    }

    @Bean
    public IntegrationFlow inboundFlow(GenericTransformer bytesToString, ConnectionRegistry registry) {
        return IntegrationFlow.from(Tcp.inboundAdapter(serverCF()))
                .channel(fromTcp())
                .transform(bytesToString)
                .route(Message.class, msg -> {
                    String connectionId = (String) msg.getHeaders().get(IpHeaders.CONNECTION_ID);
                    return registry.hasSignon(connectionId) ? "message" : "signon";
                }, mapping -> mapping
                        .subFlowMapping("signon", flow -> flow
                                .transform((String payload) -> getPartnerId(payload))
                                .handle((payload, header) -> {
                                    String connectionId = (String) header.get(IpHeaders.CONNECTION_ID);
                                    String partnerId = (String) payload;
                                    if(partnerId != null){
                                        registry.register(connectionId, partnerId);
                                        return "PARTNER_ID=" + partnerId + " SIGN-ON SUCCESSFUL";
                                    }else{
                                        return "MISSING PARTNER_ID ON SIGN-ON";
                                    }
                                })
                                .channel("toTcp"))
                        .subFlowMapping("message", flow -> flow
                                .channel("incomingFromTcp"))
                ).get();
    }

    /*
    @Bean
    public AbstractClientConnectionFactory clientCF(){
        return new TcpNetClientConnectionFactory("localhost", this.port);
    }*/

    @Bean
    public GenericTransformer<byte[], String> bytesToString() {
        return bytes -> new String(bytes, StandardCharsets.UTF_8);
    }
}
