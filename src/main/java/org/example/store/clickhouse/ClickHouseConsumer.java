package org.example.store.clickhouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cdimascio.dotenv.Dotenv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.store.HackerNewsItem;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Slf4j
public final class ClickHouseConsumer {

    private static final Dotenv DOTENV = Dotenv.configure().filename("config.env").load();

    private static final String INPUT_TOPIC = DOTENV.get("KAFKA_INPUT_TOPIC", "HackerNews-items-raw");

    private static final Properties RECIEVE_PROPERTIES = createRecieveProperties();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Contract(pure = true)
    private static @NotNull Properties createRecieveProperties() {
        final var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + DOTENV.get("EXTERNAL_PORT", "9092"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "gradle-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return properties;
    }

    public void run() {
        try (
            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(RECIEVE_PROPERTIES);
            final var clickHouseService = new ClickHouseService()
        ) {
            consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
            log.info("Started ClickHouse consumer on topic: {}", INPUT_TOPIC);

            while (true) {
                final var records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    continue;
                }

                final List<HackerNewsItem> items = new ArrayList<>();
                for (var record : records) {
                    final var item = MAPPER.readValue(record.value(), HackerNewsItem.class);
                    items.add(item);
                }

                if (!items.isEmpty()) {
                    clickHouseService.insertBatch(items);
                    log.info("Successfully insert {} items to ClickHouse", items.size());
                }
            }
        } catch (Exception e) {
            log.error("Errors while save to ClickHouse {}", e.getMessage(), e);
        }
    }
}
