package com.dexecr.kafka.manager.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import io.micronaut.core.annotation.Introspected;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor
@Builder
@Introspected
public class ConsumerGroupMember {
    private String id;
    private String clientId;
    private String host;
}
