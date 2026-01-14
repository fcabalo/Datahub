package com.db.datahubpoc.integration;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class Partner {
    String id;
    String name;
    String region;
    Date creationDate;
}
