package com.db.datahubpoc.common.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Header {
    private String source;
    private String destination;
    private String region;
    private String formatType;

    public String toJsonString(){
        return "{" +
                "\"source\":\"" + this.source + "\"," +
                "\"destination\":\"" + this.destination + "\"," +
                "\"region\":\"" + this.region + "\"," +
                "\"formatType\":\"" + this.formatType + "\"," +
                "}";
    }

    public String toString(){
        return "source: " + this.source
                + " destination: " + this.destination
                + " region: " + this.region
                + " formatType: " + this.formatType;
    }
}
