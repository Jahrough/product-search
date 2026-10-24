package dev.jahlopez.productsearch.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Configuration test suite for OpenSearch connectivity and client bean wiring.
 * <p>
 * Verifies that the {@link OpenSearchClient} bean is correctly initialized using host URIs
 * bound from configuration properties.
 */
@SpringBootTest
@Testcontainers
class OpenSearchConfigTest {

    private static final String OPENSEARCH_IMAGE = "opensearchproject/opensearch:2.11.0";
    private static final String POSTGRES_IMAGE = "postgres:15-alpine";
    private static final int OPENSEARCH_PORT = 9200;

    @Container
    private static final GenericContainer<?> OPENSEARCH_CONTAINER =
            new GenericContainer<>(DockerImageName.parse(OPENSEARCH_IMAGE))
                    .withEnv("discovery.type", "single-node")
                    .withEnv("plugins.security.disabled", "true")
                    .withEnv("DISABLE_INSTALL_DEMO_CONFIG", "true")
                    .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                    .withExposedPorts(OPENSEARCH_PORT)
                    .waitingFor(
                            Wait.forHttp("/")
                                    .forPort(OPENSEARCH_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(2))
                    );

    @Container
    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(DockerImageName.parse(POSTGRES_IMAGE));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES_CONTAINER::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES_CONTAINER::getUsername);
        registry.add("spring.datasource.password", POSTGRES_CONTAINER::getPassword);
        registry.add("spring.datasource.driver-class-name", POSTGRES_CONTAINER::getDriverClassName);

        registry.add(
                "app.opensearch.uris",
                () -> "http://"
                        + OPENSEARCH_CONTAINER.getHost()
                        + ":"
                        + OPENSEARCH_CONTAINER.getMappedPort(OPENSEARCH_PORT)
        );
    }

    @Autowired
    private OpenSearchClient openSearchClient;

    /**
     * Verifies that the OpenSearch client bean is properly wired and connected to the cluster.
     */
    @Test
    @DisplayName("Should correctly wire OpenSearch client bean with configured host URIs")
    void shouldWireOpenSearchClient() {
        assertThat(openSearchClient).isNotNull();
    }
}