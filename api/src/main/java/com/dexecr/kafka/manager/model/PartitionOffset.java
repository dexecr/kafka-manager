package com.dexecr.kafka.manager.model;

import javax.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import io.micronaut.core.annotation.Introspected;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Introspected
public class PartitionOffset {
    @NotNull
    private Integer partitionId;
    @NotNull
    private Long offset;
    private Long endOffset;
    private ConsumerGroupMember member;
}
