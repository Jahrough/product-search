package dev.jahlopez.productsearch.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link OpenSearchConfig}.
 *
 * <p>Verifies that the application creates an {@link OpenSearchClient} using
 * the configured OpenSearch URI and that the client can communicate with a
 * real OpenSearch instance.</p>
 *
 * <p>The test deliberately loads a minimal Spring context rather than the
 * complete application. This keeps the test independent of Kafka, PostgreSQL,
 * and other infrastructure unrelated to OpenSearch client configuration.</p>
 *
 * <p>Docker must be available for Testcontainers to start the OpenSearch
 * container.</p>
 */
@SpringJUnitConfig(OpenSearchConfigTest.TestConfig.class)
@Testcontainers
class OpenSearchConfigTest {

    private static final int OPENSEARCH_PORT = 9200;

    private static final String OPENSEARCH_IMAGE =
            "opensearchproject/opensearch:2.11.0";

    /**
     * Runs an isolated, single-node OpenSearch instance for integration tests.
     *
     * <p>Security is disabled only to simplify local test connectivity. Never
     * copy these settings into a production deployment.</p>
     */
    @Container
    static final GenericContainer<?> OPENSEARCH_CONTAINER =
            new GenericContainer<>(DockerImageName.parse(OPENSEARCH_IMAGE))
                    .withEnv("discovery.type", "single-node")
                    .withEnv("plugins.security.disabled", "true")
                    .withEnv("DISABLE_INSTALL_DEMO_CONFIG", "true")
                    .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                    .withExposedPorts(OPENSEARCH_PORT)
                    .waitingFor(
                            // Wait until OpenSearch responds successfully over HTTP.
                            Wait.forHttp("/")
                                    .forPort(OPENSEARCH_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(2))
                    );

    @Autowired
    private OpenSearchClient openSearchClient;

    /**
     * Supplies the dynamically mapped container URI to Spring before the
     * application context creates the OpenSearch client.
     *
     * <p>Using the mapped host port avoids assuming that port 9200 is free
     * on the machine running the test.</p>
     */
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add(
                "app.opensearch.uris",
                () -> "http://"
                        + OPENSEARCH_CONTAINER.getHost()
                        + ":"
                        + OPENSEARCH_CONTAINER.getMappedPort(OPENSEARCH_PORT)
        );
    }

    /**
     * Imports only the configuration under test, avoiding unrelated
     * application beans and external dependencies.
     */
    @Configuration(proxyBeanMethods = false)
    @Import(OpenSearchConfig.class)
    static class TestConfig {
    }

    @Test
    @DisplayName("Should create the OpenSearch client bean")
    void shouldCreateOpenSearchClient() {
        assertThat(openSearchClient)
                .as("OpenSearch client should be registered in the Spring context")
                .isNotNull();
    }

    @Test
    @DisplayName("Should retrieve cluster information from OpenSearch")
    void shouldConnectToOpenSearchCluster() throws Exception {
        // This request verifies connectivity, not just successful bean creation.
        var response = openSearchClient.info();

        assertThat(response)
                .as("OpenSearch should return cluster information")
                .isNotNull();

        assertThat(response.clusterName())
                .as("OpenSearch cluster name should be present")
                .isNotBlank();
    }
}