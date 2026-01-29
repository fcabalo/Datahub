package com.db.adapter.monitoring;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class OutgoingMessageMetric {
    private static final Logger log = LoggerFactory.getLogger(OutgoingMessageMetric.class);

    private final Meter.MeterProvider<Counter> sentMessagesCounter;

    public OutgoingMessageMetric(MeterRegistry meterRegistry){
        sentMessagesCounter = Counter.builder("adapter.message.sent.count")
                .description("Messages sent to connected partner interface")
                .withRegistry(meterRegistry);
        log.info("Metric added: {}", "adapter.message.sent.count");
    }

    public void incrementMessageSent(String parterInterfaceId){
        sentMessagesCounter.withTag("partner.interface.id", parterInterfaceId).increment();
    }
}
