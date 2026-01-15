package com.db.datahubpoc.common.config;

import com.db.datahubpoc.integration.Partner;
import com.db.datahubpoc.integration.PartnerInterface;
import com.db.datahubpoc.integration.RoutingCriteria;
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

    private ObjectMapper objectMapper = new ObjectMapper();

    @Value(value="${data.source.partnerInterface}")
    private String partnerInterfaceSource;

    @Value(value="${data.source.partner}")
    private String partnerSource;

    @Value(value="${data.source.routingCriteria}")
    private String routingCriteriaSource;

    @Bean
    public List<Partner> partners(){
        TypeReference<List<Partner>> jacksonTypeReference = new TypeReference<List<Partner>>() {};
        InputStream is = PartnerIntegrationConfig.class.getResourceAsStream(partnerSource);
        List<Partner> partners = objectMapper.readValue(is, jacksonTypeReference);
        partners.forEach(System.out::println);
        return partners;
    };

    @Bean
    @DependsOn("partners")
    public Map<String, PartnerInterface> partnerInterfaces(){
        TypeReference<List<PartnerInterface>> jacksonTypeReference = new TypeReference<List<PartnerInterface>>() {};
        InputStream is = PartnerIntegrationConfig.class.getResourceAsStream(partnerInterfaceSource);
        List<PartnerInterface> partnerInterfaceList = objectMapper.readValue(is, jacksonTypeReference);
        partnerInterfaceList.forEach(System.out::println);
        return partnerInterfaceList.stream().collect(Collectors.toConcurrentMap(PartnerInterface::getId, Function.identity()));
    }

    @Bean
    @DependsOn("partnerInterfaces")
    public List<RoutingCriteria> routingCriterias(){
        TypeReference<List<RoutingCriteria>> jacksonTypeReference = new TypeReference<List<RoutingCriteria>>() {};
        InputStream is = PartnerIntegrationConfig.class.getResourceAsStream(routingCriteriaSource);
        List<RoutingCriteria> routingCriteria = objectMapper.readValue(is, jacksonTypeReference);
        routingCriteria.forEach(System.out::println);
        return routingCriteria;
    }
}
