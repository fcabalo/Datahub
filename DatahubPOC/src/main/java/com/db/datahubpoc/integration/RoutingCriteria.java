package com.db.datahubpoc.integration;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RoutingCriteria {
    Integer id;
    Integer partnerId;
    Integer partnerInterfaceId;
    Operation messageTypeOp;
    String messageType;
    Operation recipientRegionOp;
    String recipientRegion;

    public enum Operation{
        EQUALS, NOT_EQUALS, IN, NOT_IN;
    }
}
