package com.db.adapter.common.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.ip.dsl.Tcp;
import org.springframework.integration.ip.tcp.connection.*;
import org.springframework.integration.ip.tcp.serializer.ByteArrayCrLfSerializer;
import org.springframework.messaging.MessageChannel;


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

    @Bean
    public IntegrationFlow inboundFlow() {
        return IntegrationFlow.from(Tcp.inboundAdapter(serverCF()))
                .channel(fromTcp())
                .get();
    }

    /*
    @Bean
    public AbstractClientConnectionFactory clientCF(){
        return new TcpNetClientConnectionFactory("localhost", this.port);
    }*/
}
