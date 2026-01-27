package com.db.datahubpoc.processor.service;

import com.db.datahubpoc.common.entity.DatahubMessage;
import com.db.datahubpoc.integration.PartnerInterface;
import com.db.datahubpoc.integration.RoutingCriteria;
import io.micrometer.core.annotation.Timed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class MessageProcessingService {

    @Autowired
    private Map<Integer, PartnerInterface> partnerInterfaces;

    @Autowired
    private List<RoutingCriteria> routingCriteria;

    @Timed("routing.processor.time")
    public List<PartnerInterface> getOutgoingPartnerInterfaces(DatahubMessage message){
        List<PartnerInterface> outgoingPartners = routingCriteria.stream()
                .filter(rc -> rc.getPartnerId() == null
                        || (message.getHeader().getDestination() != null  && rc.getPartnerId().equals(message.getHeader().getDestination())))
                .filter(rc -> {
                    return switch(rc.getRecipientRegionOp()){
                        case null -> true;
                        case EQUALS -> rc.getRecipientRegion().equals(message.getHeader().getRegion());
                        case NOT_EQUALS -> !rc.getRecipientRegion().equals(message.getHeader().getRegion());
                        case IN -> rc.getRecipientRegion().contains(message.getHeader().getRegion());
                        case NOT_IN -> !rc.getRecipientRegion().contains(message.getHeader().getRegion());
                    };
                })
                .filter(rc -> {
                            return switch(rc.getMessageTypeOp()){
                                case null -> true;
                                case EQUALS -> rc.getMessageType().equals(message.getHeader().getMessageType());
                                case NOT_EQUALS -> !rc.getMessageType().equals(message.getHeader().getMessageType());
                                case IN -> rc.getMessageType().contains(message.getHeader().getMessageType());
                                case NOT_IN -> !rc.getMessageType().contains(message.getHeader().getMessageType());
                            };
                        }
                )
                .map(RoutingCriteria::getPartnerInterfaceId)
                .map(pi -> partnerInterfaces.get(pi))
                .filter(pi -> !pi.getStatus().equals(PartnerInterface.Status.INACTIVE))
                .collect(Collectors.toList());
        if(outgoingPartners.isEmpty()){
            // Add dead-letter topic as default
            outgoingPartners.add(partnerInterfaces.get(2));
        }

        return outgoingPartners;
    }

    @Timed("conversion.process.time")
    public String convertMessage(DatahubMessage message, PartnerInterface pi){
        String convertedMessage;
        ObjectMapper objectMapper = new ObjectMapper();

        switch (pi.getFormatType()){
            case null -> convertedMessage = objectMapper.writeValueAsString(message);
            case "UIC" -> convertedMessage = objectMapper.writeValueAsString(message);
            case "TAF/TAP" -> convertedMessage = objectMapper.writeValueAsString(message);
            default -> convertedMessage = objectMapper.writeValueAsString(message);
        }

        return convertedMessage;
    }
}
