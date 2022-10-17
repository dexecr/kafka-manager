package com.dexecr.kafka.manager;

import io.micronaut.runtime.Micronaut;
import io.micronaut.runtime.server.EmbeddedServer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaManager {

    @SneakyThrows
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try {
            var app = Micronaut.build(args).classes(KafkaManager.class).eagerInitSingletons(true)
                    .build().start().getBean(EmbeddedServer.class).start();
            log.info("Startup completed in {}ms. Server Running: {}", System.currentTimeMillis() - start, app.getURL());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            System.exit(1);
        }

    }
}
