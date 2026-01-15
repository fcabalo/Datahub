package com.db.datahubpoc.integration;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RoutingCriteria {
    String id;
    String partnerId;
    String partnerInterfaceId;
    String formatType;
    String recipientRegion;
}
