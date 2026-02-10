package org.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.store.redis.RedisService;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class ProducerAndConsumer {

    private static final Properties SEND_PROPERTIES = createSendProperties();

    private static final Properties RECIEVE_PROPERTIES = createRecieveProperties();

    private static final String topic = "multi-part-topic";

    private final AtomicInteger counter = new AtomicInteger(0);

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
        try (
            final KafkaProducer<String, String> producer = new KafkaProducer<>(SEND_PROPERTIES);
            final var redisService = new RedisService()
        ) {
            final var count = counter.getAndIncrement();
            final var storedMessage = redisService.getMessage(String.valueOf(count));
            final var message = (storedMessage == null)
                ? "Hello from Kafka and Redis!"
                : storedMessage;

            final var record = new ProducerRecord<>(topic, "key" + count % 3, message + "; Counter: " + count);
            producer.send(record);

            log.info("Send message '{}'", message);
        } catch (Exception e) {
            log.error("Errors while send {}", e.getMessage(), e);
        }
    }

    private void receive() {
        try (
            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(RECIEVE_PROPERTIES);
            final var redisService = new RedisService()
        ) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    redisService.saveMessage("{msg}:" + counter.get(), record.value());
                    log.info(
                        "Receive message with Key: {}, Value: {} (Partition: {})",
                        record.key(),
                        record.value(),
                        record.partition()
                    );
                });

                if (!records.isEmpty()) {
                    return;
                }
            }
        } catch (Exception e) {
            log.error("Errors while receive {}", e.getMessage(), e);
        }
    }
}
