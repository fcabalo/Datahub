package com.db.datahubpoc.common.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class DatahubMessage {
    private Integer partnerId;
    private String formatType;
    private String body;

    public String toString(){
        return "Partner ID: " + this.partnerId
                + " Format Type: " + this.formatType
                + " Body: " + this.body;
    }

    public String toJsonString(){
        return "{" +
                "\"partnerId\":\"" + this.partnerId + "\"," +
                "\"formatType\":\"" + this.formatType + "\"," +
                "\"body\":\"" + this.body +
                "}";
    }

    @JsonIgnore
    public String getIncomingTopic(){
        return "Partner" + partnerId + "Incoming";
    }
}
