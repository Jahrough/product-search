package dev.jahlopez.productsearch.config;

import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

/**
 * Configures the official OpenSearch Java Client
 * and its REST transport layer.
 */
@Configuration
public class OpenSearchConfig {

    @Value("${app.opensearch.uris:http://localhost:9200}")
    private String serverUri;

    @Bean
    public RestClient restClient() {
        URI uri = URI.create(serverUri);

        String host = uri.getHost() != null
                ? uri.getHost()
                : "localhost";

        int port = uri.getPort() != -1
                ? uri.getPort()
                : 9200;

        String scheme = uri.getScheme() != null
                ? uri.getScheme()
                : "http";

        return RestClient.builder(
                new HttpHost(host, port, scheme)
        ).build();
    }

    @Bean
    public OpenSearchClient openSearchClient(RestClient restClient) {
        RestClientTransport transport = new RestClientTransport(
                restClient,
                new JacksonJsonpMapper()
        );

        return new OpenSearchClient(transport);
    }
}