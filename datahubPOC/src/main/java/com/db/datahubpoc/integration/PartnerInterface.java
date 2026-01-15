package com.db.datahubpoc.integration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class PartnerInterface {
    Integer id;
    Integer partnerId;
    InterfaceType interfaceType;
    String formatType;
    String topicName;
    String region;
    String ipAddress;
    Integer port;
    String endpoint;
    Direction direction;
    Status status;

    public PartnerInterface(){
        status = Status.LIVE;
    }

    public String getTopicName(){
        return "PI" + id + (direction.equals(Direction.INCOMING) ? "Incoming" : "Outgoing");
    }

    public enum Direction{
        INCOMING, OUTGOING;
    }

    public enum InterfaceType{
        TCPIP, RESTAPI, SOAP, DEFAULT;
    }

    public enum Status{
        LIVE, STANDBY, INACTIVE
    }
}
