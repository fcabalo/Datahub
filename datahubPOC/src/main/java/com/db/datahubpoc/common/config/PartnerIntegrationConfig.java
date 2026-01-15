package com.db.datahubpoc.common.config;

import com.db.datahubpoc.integration.Partner;
import com.db.datahubpoc.integration.PartnerInterface;
import com.db.datahubpoc.integration.RoutingCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Configuration
public class PartnerIntegrationConfig {

    private static final Logger log = LoggerFactory.getLogger(PartnerIntegrationConfig.class);

    private ObjectMapper objectMapper = new ObjectMapper();

    @Value(value="${data.source.partnerInterface}")
    private String partnerInterfaceSource;

    @Value(value="${data.source.partner}")
    private String partnerSource;

    @Value(value="${data.source.routingCriteria}")
    private String routingCriteriaSource;

    @Bean
    public List<Partner> partners(){
        log.info("Loading partners from {}", partnerSource);

        TypeReference<List<Partner>> jacksonTypeReference = new TypeReference<List<Partner>>() {};
        InputStream is = PartnerIntegrationConfig.class.getResourceAsStream(partnerSource);
        List<Partner> partners = objectMapper.readValue(is, jacksonTypeReference);
        partners.forEach(p -> log.debug("Loaded partner: {}", p));

        log.info("Loaded {} partners", partners.size());
        return partners;
    };

    @Bean
    @DependsOn("partners")
    public Map<String, PartnerInterface> partnerInterfaces(){
        log.info("Loading partner interfaces from {}", partnerInterfaceSource);

        TypeReference<List<PartnerInterface>> jacksonTypeReference = new TypeReference<List<PartnerInterface>>() {};
        InputStream is = PartnerIntegrationConfig.class.getResourceAsStream(partnerInterfaceSource);
        List<PartnerInterface> partnerInterfaceList = objectMapper.readValue(is, jacksonTypeReference);

        partnerInterfaceList.forEach(pi -> log.info("{}", pi));

        log.info("Loaded {} partner interfaces", partnerInterfaceList.size());
        return partnerInterfaceList.stream().collect(Collectors.toConcurrentMap(PartnerInterface::getId, Function.identity()));
    }

    @Bean
    @DependsOn("partnerInterfaces")
    public List<RoutingCriteria> routingCriterias() {
        log.info("Loading routing criteria from {}", routingCriteriaSource);

        TypeReference<List<RoutingCriteria>> jacksonTypeReference = new TypeReference<List<RoutingCriteria>>() {};
        InputStream is = PartnerIntegrationConfig.class.getResourceAsStream(routingCriteriaSource);
        List<RoutingCriteria> routingCriteria = objectMapper.readValue(is, jacksonTypeReference);
        
        routingCriteria.forEach(rc -> log.info("{}", rc));

        log.info("Loaded {} routing criteria", routingCriteria.size());
        return routingCriteria;
    }
}
