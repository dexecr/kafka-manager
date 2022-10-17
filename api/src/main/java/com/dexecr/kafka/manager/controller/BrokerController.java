package com.dexecr.kafka.manager.controller;

import java.util.Set;

import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.service.BrokerService;
import com.dexecr.kafka.manager.service.ClusterInfoProvider;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import reactor.core.publisher.Mono;
import com.dexecr.kafka.manager.model.Broker;

@RequiredArgsConstructor
@Controller("/brokers")
public class BrokerController implements ClusterProviderController {

    private final BrokerService brokerService;
    @Getter
    private final ClusterInfoProvider clusterInfoProvider;

    @Get("{?cluster}")
    public Mono<Set<Broker>> getAll(@QueryValue String cluster) {
        return withCluster(cluster, brokerService::getBrokers);
    }

}
