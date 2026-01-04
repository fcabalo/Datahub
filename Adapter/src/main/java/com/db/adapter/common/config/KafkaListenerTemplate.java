package com.db.adapter.common.config;

import org.springframework.kafka.listener.MessageListener;

public class KafkaListenerTemplate implements MessageListener {



    @Override
    public void onMessage(Object data) {
        System.out.println("RECORD PROCESSING: " + data);
    }
}
