package com.dexecr.kafka.manager.model;

import java.util.Map;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import io.micronaut.core.annotation.Introspected;

@NoArgsConstructor
@Data
@AllArgsConstructor
@Introspected
public class Reassignment {
    private Map<String, Short> topicsAndReplication;
    private Set<Integer> brokers;
}
