package org.example;

import org.example.kafka.ProducerAndConsumer;

public final class Main {

    static void main() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", "warn");

        final var producerAndConsumer = new ProducerAndConsumer();
        producerAndConsumer.run();
    }
}
