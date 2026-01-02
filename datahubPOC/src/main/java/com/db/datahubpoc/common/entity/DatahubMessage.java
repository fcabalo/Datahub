package com.db.datahubpoc.common.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class DatahubMessage {
    private Header header;
    private String body;

    public String toString(){
        return "Header: " + this.header.toString()
                + " Body: " + this.body;
    }

    public String toJsonString(){
        return "{" +
                "\"header\":\"" + this.header.toJsonString() + "\"," +
                "\"body\":\"" + this.body +
                "}";
    }

    @JsonIgnore
    public String getIncomingTopic(){
        return "Partner" + this.header.getPartnerId() + "Incoming";
    }

    @JsonIgnore
    public String getFormatType(){
        return this.header.getFormatType();
    }
}
