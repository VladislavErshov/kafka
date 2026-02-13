package org.example.store.qdrant;

import io.github.cdimascio.dotenv.Dotenv;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.VectorParams;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutionException;

@Slf4j
public final class QdrantService implements AutoCloseable {

    private static final String COLLECTION_NAME = "hackernews_vectors";

    private final @NotNull QdrantClient client;

    @Contract(pure = true)
    public QdrantService() {
        final var dotenv = Dotenv.configure().filename("config.env").load();
        final var host = "127.0.0.1";
        final var port = Integer.parseInt(dotenv.get("QDRANT_GRPC_PORT", "6334"));

        this.client = new QdrantClient(QdrantGrpcClient.newBuilder(host, port, false).build());
        log.info("Connected to Qdrant on {}:{}", host, port);
    }

    public void createCollectionIfNotExists(int vectorSize) throws ExecutionException, InterruptedException {
        final boolean exists = client.collectionExistsAsync(COLLECTION_NAME).get();
        if (exists) {
            log.info("Collection {} already exists", COLLECTION_NAME);
            return;
        }

        log.info("Creating collection: {}", COLLECTION_NAME);
        final var params = VectorParams.newBuilder()
            .setDistance(Distance.Cosine)
            .setSize(vectorSize)
            .build();
        client.createCollectionAsync(COLLECTION_NAME, params).get();
    }

    @Override
    public void close() {
        client.close();
        log.info("Qdrant connection successfully closed");
    }
}
