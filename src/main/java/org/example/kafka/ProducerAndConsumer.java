package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public final class ProducerAndConsumer {

    private static final Properties SEND_PROPERTIES = createSendProperties();

    private static final Properties RECIEVE_PROPERTIES = createRecieveProperties();

    private final AtomicInteger counter = new AtomicInteger(0);

    private final String topic = "multi-part-topic";

    @Contract(pure = true)
    private static @NotNull Properties createSendProperties() {
        final var properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return properties;
    }

    @Contract(pure = true)
    private static @NotNull Properties createRecieveProperties() {
        final var properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "gradle-group");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return properties;
    }

    public void run() {
        while (counter.get() < 100) {
            send();
            receive();
        }
    }

    private void send() {
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(SEND_PROPERTIES)) {
            final var count = counter.getAndIncrement();
            final ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                "key" + count % 3,
                "Hello from Gradle! Counter: " + count
            );
            producer.send(record);
        } catch (Exception e) {
            // Not need handle
        }
    }

    private void receive() {
        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(RECIEVE_PROPERTIES)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> System.out.printf(
                    "Key: %s, Value: %s (Partition: %d)%n",
                    record.key(),
                    record.value(),
                    record.partition()
                ));

                if (!records.isEmpty()) {
                    return;
                }
            }
        }
    }
}
