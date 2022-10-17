package com.dexecr.kafka.manager.model;

import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.bind.annotation.Bindable;

import java.time.Duration;

@Introspected
public interface Cluster {

    @Bindable(defaultValue = "default")
    String getName();

    @Bindable
    String getServers();

    @Bindable(defaultValue = "30s")
    Duration getRequestTimeout();

    @Bindable(defaultValue = "5s")
    Duration getCloseConnectionTimeout();

    @Bindable(defaultValue = "1m")
    Duration getExpirationClientPeriod();
}
