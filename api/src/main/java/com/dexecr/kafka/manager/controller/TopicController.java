package com.dexecr.kafka.manager.controller;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.service.ClusterInfoProvider;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Put;
import io.micronaut.http.annotation.QueryValue;
import reactor.core.publisher.Mono;
import com.dexecr.kafka.manager.model.Topic;
import com.dexecr.kafka.manager.model.TopicConfiguration;
import com.dexecr.kafka.manager.service.TopicService;

@RequiredArgsConstructor
@Controller("/topics")
public class TopicController implements ClusterProviderController {

    private final TopicService topicService;
    @Getter
    private final ClusterInfoProvider clusterInfoProvider;

    @Get("{?cluster}")
    public Mono<Set<String>> getAll(@QueryValue String cluster) {
        return withCluster(cluster, topicService::getTopicNames);
    }

    @Get("/{name}{?cluster}")
    public Mono<Topic> getTopic(@QueryValue String cluster, @PathVariable String name) {
        return withCluster(cluster, c -> topicService.getTopic(c, name));
    }

    @Delete("/{name}{?cluster}")
    public Mono<Void> deleteTopic(@QueryValue String cluster, @PathVariable String name) {
        return withCluster(cluster, c -> topicService.deleteTopic(c, name));
    }

    @Post("{?cluster}")
    public Mono<Void> create(@QueryValue String cluster, @Body Topic topic) {
        return withCluster(cluster, c -> topicService.createTopic(c, topic));
    }

    @Get("/{name}/config{?cluster}")
    public Mono<TopicConfiguration> getTopicConfig(@QueryValue String cluster, @PathVariable String name) {
        return withCluster(cluster, c -> topicService.getTopicConfig(c, name));
    }

    @Put("/{name}/config{?cluster}")
    public Mono<Void> setConfigByName(@QueryValue String cluster, @PathVariable String name, @Body TopicConfiguration config) {
        return withCluster(cluster, c -> topicService.updateTopicConfig(c, name, config));
    }

    @Put("/{name}/partitions/{count}{?cluster}")
    public Mono<Void> increasePartitionsCount(@QueryValue String cluster, @PathVariable String name, @PathVariable int count) {
        return withCluster(cluster, c -> topicService.increasePartitionsCount(c, name, count));
    }

    @Get("/{name}/assignment{?cluster}")
    public Mono<Map<Integer, List<Integer>>> getAssignment(@QueryValue String cluster, @PathVariable String name) {
        return withCluster(cluster, c -> topicService.getTopicAssignment(c, name));
    }

    @Get("/config-template")
    public Mono<Map<String, String>> getTopicConfigTemplate() {
        return topicService.getTopicConfigTemplate();
    }

}
