package org.example.store.redis;

import io.github.cdimascio.dotenv.Dotenv;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import redis.clients.jedis.UnifiedJedis;

@Slf4j
public final class RedisService implements AutoCloseable {

    private static final String MESSAGE_KEY = "msg:";

    private final @NotNull UnifiedJedis jedis;

    @Contract(pure = true)
    public RedisService() {
        final var dotenv = Dotenv.load();
        final var host = "127.0.0.1";
        final int port = Integer.parseInt(dotenv.get("REDIS_PORT", "6379"));

        this.jedis = new UnifiedJedis("redis://" + host + ":" + port);
        log.info("Connected to Redis on {}:{}", host, port);
    }

    public void saveMessage(@NotNull String messageId, @NotNull String content) {
        final var key = MESSAGE_KEY + messageId;
        jedis.set(key, content);
        log.info("Message stored in Redis with key: {}", key);
    }

    @Contract(pure = true)
    public @Nullable String getMessage(@NotNull String messageId) {
        return jedis.get(MESSAGE_KEY + messageId);
    }

    @Override
    public void close() {
        jedis.close();
        log.info("Redis connection successfully closed");
    }
}
