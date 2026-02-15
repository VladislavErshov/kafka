package org.example;

import org.example.store.clickhouse.ClickHouseConsumer;
import org.example.store.clickhouse.ClickHouseService;

import java.sql.SQLException;
import java.util.concurrent.Executors;

public final class Main {

    static void main() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", "warn");

        try(final var clickHouseService = new ClickHouseService()) {
            final var stories = clickHouseService.getTopStories(10);
            System.out.println(stories);
        } catch (SQLException e) {
            //
        }

        final var service = Executors.newSingleThreadExecutor();
        final var consumer = new ClickHouseConsumer();
        service.submit(consumer::run);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try(final var clickHouseService = new ClickHouseService()) {
            final var stories = clickHouseService.getTopStories(10);
            System.out.println(stories);
        } catch (SQLException e) {
            //
        }
    }
}
