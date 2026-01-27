package com.db.datahubpoc.monitoring;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class MessageProcessingMetricsService {
    private final Meter.MeterProvider<Counter> messageTypeCounter;
    private static final Logger log = LoggerFactory.getLogger(MessageProcessingMetricsService.class);

    public MessageProcessingMetricsService(MeterRegistry meterRegistry){
        messageTypeCounter = Counter.builder("ingester.message.incoming.count")
                .description("Count of all messages received")
                .withRegistry(meterRegistry);
        log.info("Metric added: {}", "ingester.message.incoming.count");
    }

    public void incrementMessageType(String messageType){
        messageTypeCounter.withTag("message.type", messageType).increment();
    }

}
