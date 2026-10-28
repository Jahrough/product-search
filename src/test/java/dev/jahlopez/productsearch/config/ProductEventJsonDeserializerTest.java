package dev.jahlopez.productsearch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

class ProductEventJsonDeserializerTest {

    private final ProductEventJsonDeserializer deserializer = new ProductEventJsonDeserializer();

    @Test
    void shouldDeserializeEventConformingToSchemaVersionOne() {
        String payload = """
                {
                  "schemaVersion": 1,
                  "eventId": "7df16f23-9bd6-4d8c-9c7c-8128f2ae8020",
                  "version": 1,
                  "productId": 123,
                  "sku": "SKU-123",
                  "name": "Keyboard",
                  "description": null,
                  "price": 149.99,
                  "stockQuantity": 42,
                  "eventType": "CREATED",
                  "timestamp": "2026-10-25T12:00:00"
                }
                """;

        var event = deserializer.deserialize(
                "product-events",
                payload.getBytes(StandardCharsets.UTF_8)
        );

        assertThat(event.schemaVersion()).isEqualTo(1);
        assertThat(event.productId()).isEqualTo(123L);
    }

    @Test
    void shouldRejectEventsWithUnsupportedSchemaVersion() {
        String payload = """
                {
                  "schemaVersion": 2,
                  "eventId": "7df16f23-9bd6-4d8c-9c7c-8128f2ae8020",
                  "version": 1,
                  "productId": 123,
                  "eventType": "DELETED",
                  "timestamp": "2026-10-25T12:00:00"
                }
                """;

        assertThatThrownBy(() -> deserializer.deserialize(
                "product-events",
                payload.getBytes(StandardCharsets.UTF_8)
        ))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("schema v1");
    }

    @Test
    void shouldRejectNonNumericProductIds() {
        String payload = """
                {
                  "schemaVersion": 1,
                  "eventId": "7df16f23-9bd6-4d8c-9c7c-8128f2ae8020",
                  "version": 1,
                  "productId": "123",
                  "eventType": "DELETED",
                  "timestamp": "2026-10-25T12:00:00"
                }
                """;

        assertThatThrownBy(() -> deserializer.deserialize(
                "product-events",
                payload.getBytes(StandardCharsets.UTF_8)
        ))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("schema v1");
    }

    @Test
    void shouldRejectMissingVersion() {
        assertSchemaRejectsVersion("");
    }

    @Test
    void shouldRejectNullNonPositiveAndNonIntegerVersions() {
        List.of(
                "\"version\": null,",
                "\"version\": 0,",
                "\"version\": -1,",
                "\"version\": \"1\",",
                "\"version\": 1.5,"
        ).forEach(this::assertSchemaRejectsVersion);
    }

    private void assertSchemaRejectsVersion(String versionProperty) {
        String payload = """
                {
                  "schemaVersion": 1,
                  "eventId": "7df16f23-9bd6-4d8c-9c7c-8128f2ae8020",
                  %s
                  "productId": 123,
                  "eventType": "DELETED",
                  "timestamp": "2026-10-25T12:00:00"
                }
                """.formatted(versionProperty);

        assertThatThrownBy(() -> deserializer.deserialize(
                "product-events",
                payload.getBytes(StandardCharsets.UTF_8)
        ))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("schema v1");
    }
}
