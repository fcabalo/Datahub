package com.db.datahubpoc.integration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class PartnerInterface {
    String id;
    String partnerId;
    String interfaceType;
    String region;
    String ipAddress;
    String port;
    String endpoint;
    String direction;
    Boolean live;

    public PartnerInterface(){
        live = true;
    }

    public String getIncomingTopic(){
        return "PI" + id + "Incoming";
    }

    public String getOutgoingTopic(){
        return "PI" + id + "Outgoing";
    }
}
