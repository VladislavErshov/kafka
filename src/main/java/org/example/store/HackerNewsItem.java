package org.example.store;

import org.jetbrains.annotations.NotNull;

public record HackerNewsItem(
    long id,
    @NotNull String title,
    @NotNull String url,
    int score,
    @NotNull String author,
    @NotNull java.sql.Timestamp time
) {}
