package dev.jahlopez.productsearch.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Data JPA slice test suite for {@link ProcessedEventRepository}.
 * <p>
 * Validates consumer-side inbox deduplication persistence and version tracking queries
 * against an isolated PostgreSQL Testcontainer instance.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ProcessedEventRepositoryTest {

    private static final String POSTGRES_IMAGE = "postgres:15-alpine";
    private static final String AGGREGATE_TYPE = "Product";
    private static final String AGGREGATE_ID = "prod-inbox-01";

    /**
     * PostgreSQL Testcontainer initialized for isolated database testing.
     */
    @Container
    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(DockerImageName.parse(POSTGRES_IMAGE));

    /**
     * Dynamically injects PostgreSQL container properties into the Spring Environment.
     */
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES_CONTAINER::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES_CONTAINER::getUsername);
        registry.add("spring.datasource.password", POSTGRES_CONTAINER::getPassword);
        registry.add("spring.datasource.driver-class-name", POSTGRES_CONTAINER::getDriverClassName);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create");
        registry.add("spring.flyway.enabled", () -> "false");
    }

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @BeforeEach
    void setUp() {
        processedEventRepository.deleteAll();
    }

    /**
     * Verifies that `findHighestProcessedVersion` correctly retrieves the maximum aggregate
     * version for a given aggregate type and ID.
     */
    @Test
    @DisplayName("Should return the highest processed aggregate version")
    void shouldReturnHighestProcessedVersion() {
        // Arrange: Save multiple processed events with varying versions for the same aggregate
        ProcessedEventEntity eventV1 = ProcessedEventEntity.builder()
                .eventId(UUID.randomUUID())
                .aggregateType(AGGREGATE_TYPE)
                .aggregateId(AGGREGATE_ID)
                .aggregateVersion(1L)
                .eventType("CREATED")
                .processedAt(OffsetDateTime.now())
                .build();

        ProcessedEventEntity eventV2 = ProcessedEventEntity.builder()
                .eventId(UUID.randomUUID())
                .aggregateType(AGGREGATE_TYPE)
                .aggregateId(AGGREGATE_ID)
                .aggregateVersion(2L)
                .eventType("UPDATED")
                .processedAt(OffsetDateTime.now())
                .build();

        processedEventRepository.save(eventV1);
        processedEventRepository.save(eventV2);

        // Act: Query highest processed version
        Optional<Long> highestVersion = processedEventRepository
                .findHighestProcessedVersion(AGGREGATE_TYPE, AGGREGATE_ID);

        // Assert: Verify version matches the maximum saved version (2L)
        assertThat(highestVersion)
                .isPresent()
                .contains(2L);
    }

    /**
     * Verifies that `findHighestProcessedVersion` returns an empty optional when no events
     * exist for the specified aggregate.
     */
    @Test
    @DisplayName("Should return empty optional when no processed events exist for aggregate")
    void shouldReturnEmptyWhenNoEventsExist() {
        Optional<Long> highestVersion = processedEventRepository
                .findHighestProcessedVersion(AGGREGATE_TYPE, "non-existent-id");

        assertThat(highestVersion).isEmpty();
    }
}