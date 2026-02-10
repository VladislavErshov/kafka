package org.example;

import org.example.kafka.ProducerAndConsumer;
import org.example.store.clickhouse.ClickHouseService;

import java.sql.SQLException;

public final class Main {

    static void main() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", "warn");

        try(final var clickHouseService = new ClickHouseService()) {
            final var stories = clickHouseService.getTopStories(10);
            System.out.println(stories);
        } catch (SQLException e) {
            //
        }

        final var producerAndConsumer = new ProducerAndConsumer();
        producerAndConsumer.run();
    }
}
