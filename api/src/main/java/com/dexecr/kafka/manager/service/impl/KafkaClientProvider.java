package com.dexecr.kafka.manager.service.impl;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PreDestroy;

import com.dexecr.kafka.clients.rx.admin.RxAdmin;
import com.dexecr.kafka.manager.model.Cluster;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.admin.AdminClientConfig;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
class KafkaClientProvider {

    private final ConcurrentMap<Cluster, CacheEntry> clients = new ConcurrentHashMap<>();
    private final BlockingQueue<CacheEntry> evictedEntries = new LinkedBlockingQueue<>();
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    public KafkaClientProvider() {
        new Thread(new CleanTask()).start();
    }

    public RxAdmin getClient(Cluster cluster) {
        var entry = clients.computeIfAbsent(cluster, this::openAdminClient);
        entry.setTime(Instant.now());
        return entry.getClient();
    }

    @PreDestroy
    public void destroy() {
        isActive.set(false);
        log.info("Clients will be closed: {}", clients.size());
        evictedEntries.addAll(clients.values());
    }

    private CacheEntry openAdminClient(Cluster cluster) {
        if (!isActive.get()) {
            throw new IllegalArgumentException("Service is shutdown...");
        }
        return new CacheEntry(cluster, RxAdmin.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getServers(),
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) cluster.getRequestTimeout().toMillis()
        )));
    }

    private void invalidateExpired() {
        clients.values().removeIf(entry -> {
            boolean flag = entry.getTime().plus(entry.cluster.getExpirationClientPeriod()).isBefore(Instant.now());
            if (flag) {
                evictedEntries.add(entry);
            }
            return flag;
        });
    }

    private class CleanTask implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    var entry = evictedEntries.poll(1, TimeUnit.SECONDS);
                    if (entry == null && hasNoActiveTasks()) {
                        break;
                    }
                    Optional.ofNullable(entry).ifPresent(cacheEntry -> {
                        cacheEntry.getClient().close(cacheEntry.cluster.getCloseConnectionTimeout());
                        log.info("Client {} was closed", cacheEntry.cluster.getServers());
                    });
                } catch (Exception e) {
                    if (hasNoActiveTasks()) {
                        break;
                    }
                }
                try {
                    invalidateExpired();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            log.info("Cleaner was stopped");
        }

        private boolean hasNoActiveTasks() {
            return evictedEntries.size() == 0 && !isActive.get();
        }
    }

    @RequiredArgsConstructor
    @Getter
    private static class CacheEntry {
        @NonNull
        private final Cluster cluster;
        @NonNull
        private final RxAdmin client;
        @Setter
        private Instant time = Instant.now();
    }

}
