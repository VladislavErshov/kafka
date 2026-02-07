package org.example.redis;

import io.github.cdimascio.dotenv.Dotenv;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.UnifiedJedis;

public final class RedisService implements AutoCloseable {

    private static final String MESSAGE_KEY = "msg:";

    private final @NotNull UnifiedJedis jedis;

    public RedisService() {
        // Load environment variables from .env file
        final var dotenv = Dotenv.load();
        final var host = "127.0.0.1";
        final int port = Integer.parseInt(dotenv.get("REDIS_PORT", "6379"));

        // Initialize UnifiedJedis for Standalone mode connection
        this.jedis = new UnifiedJedis("redis://" + host + ":" + port);
        System.out.println("Connected to Redis on " + host + ":" + port);
    }

    public void saveMessage(@NotNull String messageId, @NotNull String content) {
        final var key = MESSAGE_KEY + messageId;
        jedis.set(key, content);
        System.out.println("Message stored in Redis with key: " + key);
    }

    public String getMessage(@NotNull String messageId) {
        return jedis.get(MESSAGE_KEY + messageId);
    }

    @Override
    public void close() {
        jedis.close();
        System.out.println("Redis connection successfully closed.");
    }
}
