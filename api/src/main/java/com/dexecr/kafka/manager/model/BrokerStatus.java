package com.dexecr.kafka.manager.model;

public enum BrokerStatus {

    NOT_RUNNING,
    STARTING,
    RECOVERING_FROM_UNCLEAN_SHUTDOWN,
    RUNNING_AS_BROKER,
    RUNNING_AS_CONTROLLER,
    PENDING_SHUTDOWN,
    SHUTTING_DOWN

}
