package com.db.datahubpoc.common.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Header {
    private Integer source;
    private Integer destination;
    private String region;
    private String messageType;
    private Date receivedDate;



    public String toString(){
        return "source: " + this.source
                + " destination: " + this.destination
                + " region: " + this.region
                + " messageType: " + this.messageType
                + " receivedDate: " + this.receivedDate;
    }
}
