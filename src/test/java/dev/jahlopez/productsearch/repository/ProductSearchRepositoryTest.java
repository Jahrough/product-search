package dev.jahlopez.productsearch.repository;

import dev.jahlopez.productsearch.document.ProductDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.testcontainers.OpensearchContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test suite for {@link ProductSearchRepository}.
 * <p>
 * This class validates OpenSearch indexing, search query execution, and pagination
 * behaviors using Testcontainers to spin up isolated, production-grade OpenSearch and
 * PostgreSQL instances.
 */
@SpringBootTest
@Testcontainers
class ProductSearchRepositoryTest {

    private static final String INDEX_NAME = "products";

    /**
     * OpenSearch Testcontainer configured as a single-node cluster with security plugins
     * disabled for seamless local and CI/CD integration test execution.
     */
    @Container
    static final OpensearchContainer<?> opensearch =
            new OpensearchContainer<>(
                    DockerImageName.parse("opensearchproject/opensearch:2.11.0")
            )
                    .withEnv("discovery.type", "single-node")
                    .withEnv("DISABLE_SECURITY_PLUGIN", "true");

    /**
     * PostgreSQL Testcontainer initialized to satisfy Spring Data JPA and Flyway migration
     * requirements during full application context bootstrap.
     */
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:15-alpine")
    );

    /**
     * Dynamically injects container connection URIs and credentials into the Spring Environment
     * prior to application context initialization.
     *
     * @registry the dynamic property registry
     */
    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("app.opensearch.uris", opensearch::getHttpHostAddress);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private ProductSearchRepository productSearchRepository;

    @Autowired
    private OpenSearchClient openSearchClient;

    /**
     * Resets and recreates the OpenSearch index before each test execution to guarantee
     * state isolation and prevent test pollution.
     *
     * @throws Exception if index operations fail
     */
    @BeforeEach
    void setUp() throws Exception {
        boolean exists = openSearchClient.indices()
                .exists(ExistsRequest.of(e -> e.index(INDEX_NAME)))
                .value();

        if (exists) {
            openSearchClient.indices()
                    .delete(DeleteIndexRequest.of(d -> d.index(INDEX_NAME)));
        }

        openSearchClient.indices()
                .create(CreateIndexRequest.of(c -> c.index(INDEX_NAME)));
    }

    /**
     * Verifies that full-text queries correctly match products by name or description
     * and respect pagination bounds.
     *
     * @throws Exception if document indexing or search execution fails
     */
    @Test
    @DisplayName("Should search product documents by name or description")
    void shouldSearchByNameOrDescription() throws Exception {
        // Arrange: Build test document entities
        ProductDocument keyboard = ProductDocument.builder()
                .id("prod-repo-03")
                .sku("SKU-REPO-03")
                .name("Mechanical Keyboard")
                .description("RGB backlit keyboard with blue switches")
                .price(new BigDecimal("89.99"))
                .stockQuantity(40)
                .version(1L)
                .build();

        ProductDocument monitor = ProductDocument.builder()
                .id("prod-repo-04")
                .sku("SKU-REPO-04")
                .name("Gaming Monitor")
                .description("27-inch 144Hz gaming display")
                .price(new BigDecimal("299.99"))
                .stockQuantity(15)
                .version(1L)
                .build();

        // Seed OpenSearch directly to bypass repository abstraction boundaries for setup
        openSearchClient.index(i -> i
                .index(INDEX_NAME)
                .id(keyboard.getId())
                .document(keyboard));

        openSearchClient.index(i -> i
                .index(INDEX_NAME)
                .id(monitor.getId())
                .document(monitor));

        // Refresh index segments to make documents immediately searchable
        openSearchClient.indices()
                .refresh(r -> r.index(INDEX_NAME));

        // Act: Execute repository search query with pagination
        Page<ProductDocument> results =
                productSearchRepository.findByNameContainingOrDescriptionContaining(
                        "Mechanical",
                        "Mechanical",
                        PageRequest.of(0, 10)
                );

        // Assert: Validate matching document properties and total counts
        assertThat(results.getContent()).hasSize(1);
        assertThat(results.getContent().get(0).getId())
                .isEqualTo("prod-repo-03");
        assertThat(results.getContent().get(0).getSku())
                .isEqualTo("SKU-REPO-03");
    }
}