package com.db.adapter.interfaces;

import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.annotation.Transformer;
import org.springframework.stereotype.Service;

@Service
@MessageEndpoint
public class AdapterServer {

    @Transformer(inputChannel = "fromTcp", outputChannel = "toTcp")
    public String echoMessage(byte[] bytes){
        return new String(bytes);
    }
}
