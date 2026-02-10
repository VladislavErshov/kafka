package org.example.store.clickhouse;

import com.clickhouse.jdbc.ClickHouseDataSource;
import io.github.cdimascio.dotenv.Dotenv;
import lombok.extern.slf4j.Slf4j;
import org.example.store.HackerNewsItem;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public final class ClickHouseService implements AutoCloseable {

    private final @NotNull ClickHouseDataSource dataSource;

    @Contract(pure = true)
    public ClickHouseService() throws SQLException {
        final var dotenv = Dotenv.load();

        final var host = "127.0.0.1";
        final var port = dotenv.get("CLICKHOUSE_HTTP_PORT", "8123");
        final var url = String.format("jdbc:ch://%s:%s/default?compress=false", host, port);

        final var properties = new Properties();
        final var user = dotenv.get("CLICKHOUSE_USER", "default");
        properties.setProperty("user", user);
        properties.setProperty("password", dotenv.get("CLICKHOUSE_PASSWORD", ""));

        this.dataSource = new ClickHouseDataSource(url, properties);
        log.info("Connected to ClickHouse on {}:{} as user {}", host, port, user);
    }

    @Contract(pure = true)
    public @NotNull List<HackerNewsItem> getTopStories(int limit) {
        final List<HackerNewsItem> stories = new ArrayList<>();
        final var sql =
            String.format("SELECT id, title, url, score, by, time FROM hackernews ORDER BY score DESC LIMIT %d", limit);

        try (
            final var connection = dataSource.getConnection();
            final var statement = connection.createStatement();
            final var result = statement.executeQuery(sql)
        ) {
            while (result.next()) {
                stories.add(new HackerNewsItem(
                    result.getLong("id"),
                    result.getString("title"),
                    result.getString("url"),
                    result.getInt("score"),
                    result.getString("by"),
                    result.getTimestamp("time")
                ));
            }

            log.info("Successfully fetched {} stories from ClickHouse", stories.size());
        } catch (Exception e) {
            log.error("Failed to fetch stories from ClickHouse: {}", e.getMessage(), e);
        }

        return stories;
    }

    @Override
    public void close() {
        log.info("ClickHouse connection successfully closed");
    }
}
