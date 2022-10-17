package com.dexecr.kafka.manager.factory;

import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.service.ClusterInfoProvider;
import com.dexecr.kafka.manager.service.impl.PropertyClusterInfoProvider;
import io.micronaut.context.annotation.*;
import io.micronaut.core.annotation.Introspected;
import jakarta.inject.Singleton;

import java.util.List;

@Factory
class ClusterProviderInfoFactory {

    @Singleton
    @Requires(property = "kafka.cluster-provider", value = "properties")
    @Requires(property = "kafka.clusters")
    ClusterInfoProvider clusterInfoProvider(List<ClusterPropertiesBean> clusters) {
        return new PropertyClusterInfoProvider(clusters.stream().map(Cluster.class::cast).toList());
    }

    @Introspected
    @Requires(property = "kafka.cluster-provider", value = "properties")
    @EachProperty(value = "kafka.clusters", list = true)
    private interface ClusterPropertiesBean extends Cluster {}

    @Singleton
    @Requires(property = "bootstrap.servers")
    ClusterInfoProvider clusterInfoProvider(ClusterCliPropertiesBean bean) {
        return new PropertyClusterInfoProvider(List.of(bean));
    }

    @Introspected
    @Requires(property = "bootstrap.servers")
    @ConfigurationProperties(value = "bootstrap")
    private interface ClusterCliPropertiesBean extends Cluster {}

}
