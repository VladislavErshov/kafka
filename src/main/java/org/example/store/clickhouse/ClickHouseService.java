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
        final var dotenv = Dotenv.configure().filename("config.env").load();
        final var secrets = Dotenv.configure().filename(".env").load();

        final var host = "127.0.0.1";
        final var port = dotenv.get("CLICKHOUSE_HTTP_PORT", "8123");
        final var url = String.format("jdbc:ch://%s:%s/default?compress=false", host, port);

        final var properties = new Properties();
        final var user = dotenv.get("CLICKHOUSE_USER", "default");
        properties.setProperty("user", user);
        properties.setProperty("password", secrets.get("CLICKHOUSE_PASSWORD", ""));

        this.dataSource = new ClickHouseDataSource(url, properties);
        log.info("Connected to ClickHouse on {}:{} as user {}", host, port, user);
    }

    @Contract(pure = true)
    public @NotNull List<HackerNewsItem> getTopStories(int limit) {
        initTable();

        final List<HackerNewsItem> stories = new ArrayList<>();
        final var sql =
            String.format("SELECT id, title, url, score, author, time FROM hackernews ORDER BY score DESC LIMIT %d", limit);

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
                    result.getString("author"),
                    result.getTimestamp("time")
                ));
            }

            log.info("Successfully fetched {} stories from ClickHouse", stories.size());
        } catch (Exception e) {
            log.error("Failed to fetch stories from ClickHouse: {}", e.getMessage(), e);
        }

        return stories;
    }

    public void insertBatch(@NotNull List<HackerNewsItem> items) {
        initTable();

        final var sql = "INSERT INTO hackernews (id, title, url, score, author, time) VALUES (?, ?, ?, ?, ?, ?)";
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(sql)
        ) {
            for (var item : items) {
                statement.setLong(1, item.id());
                statement.setString(2, item.title());
                statement.setString(3, item.url());
                statement.setInt(4, item.score());
                statement.setString(5, item.author());
                statement.setTimestamp(6, item.time());
                statement.addBatch();
            }
            statement.executeBatch();

            log.info("Batch inserted {} items to ClickHouse", items.size());
        } catch (SQLException e) {
            log.error("ClickHouse batch insert failed: {}", e.getMessage());
        }
    }

    private void initTable() {
        final var sql = """
        CREATE TABLE IF NOT EXISTS hackernews (
            id UInt64,
            title String,
            url String,
            score Int32,
            author String,
            time DateTime
        ) ENGINE = MergeTree() 
        ORDER BY id""";

        try (
            final var connection = dataSource.getConnection();
            final var statement = connection.createStatement()
        ) {
            statement.execute(sql);
            log.info("Table 'hackernews' is ready");
        } catch (SQLException e) {
            log.error("Failed to initialize table: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        log.info("ClickHouse connection successfully closed");
    }
}
