package com.dexecr.kafka.manager.controller;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.dexecr.kafka.manager.service.ClusterInfoProvider;
import io.micronaut.core.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Put;
import io.micronaut.http.annotation.QueryValue;
import reactor.core.publisher.Mono;
import com.dexecr.kafka.manager.model.Reassignment;
import com.dexecr.kafka.manager.model.TopicReassignmentInfo;
import com.dexecr.kafka.manager.service.ReassignmentService;


@RequiredArgsConstructor
@Controller("/reassign")
public class ReassignmentController implements ClusterProviderController {
    private final ReassignmentService reassignmentService;
    @Getter
    private final ClusterInfoProvider clusterInfoProvider;

    @Get("{?cluster}")
    public Mono<List<TopicReassignmentInfo>> getReassignmentStatus(String cluster) {
        return withCluster(cluster, reassignmentService::reassignmentStatus);
    }

    @Put("{?cluster}")
    public Mono<Void> addReassignment(@QueryValue String cluster, @Nullable @Body Reassignment reassignment) {
        return withCluster(cluster, c -> reassignmentService
                .addReassignment(c, Optional.ofNullable(reassignment).orElseGet(Reassignment::new)));
    }

    @Post("{?cluster}")
    public Mono<Void> clearReassignment(@QueryValue String cluster, @Body Map<String, List<Integer>> topicPartitions) {
        return withCluster(cluster, c -> reassignmentService.clearReassignment(c, topicPartitions));
    }
}
