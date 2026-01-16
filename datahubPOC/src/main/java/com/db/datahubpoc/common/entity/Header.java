package com.db.datahubpoc.common.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Header {
    private Integer source;
    private Integer destination;
    private String region;
    private String messageType;

    public String toJsonString(){
        return "{" +
                "\"source\":\"" + this.source + "\"," +
                "\"destination\":\"" + this.destination + "\"," +
                "\"region\":\"" + this.region + "\"," +
                "\"messageType\":\"" + this.messageType + "\"" +
                "}";
    }

    public String toString(){
        return "source: " + this.source
                + " destination: " + this.destination
                + " region: " + this.region
                + " messageType: " + this.messageType;
    }
}
