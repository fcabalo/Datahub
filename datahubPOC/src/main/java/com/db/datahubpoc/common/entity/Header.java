package com.db.datahubpoc.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Header {
    private Integer partnerId;
    private String formatType;

    public String toJsonString(){
        return "{" +
                "\"partnerId\":\"" + this.partnerId + "\"," +
                "\"formatType\":\"" + this.formatType + "\"," +
                "}";
    }

    public String toString(){
        return "partnerId: " + this.partnerId
                + " formatType: " + this.formatType;
    }
}
