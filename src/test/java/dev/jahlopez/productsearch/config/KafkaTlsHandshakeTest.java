package dev.jahlopez.productsearch.config;

import dev.jahlopez.productsearch.event.ProductEvent;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class KafkaTlsHandshakeTest {

    private static final String PRODUCT_TOPIC = "product-events-tls-test";
    private static final String DLT_TOPIC = PRODUCT_TOPIC + ".DLT";
    private static final String TLS_PASSWORD = "test-only-password";
    private static final String CONTAINER_KEYSTORE = "/tmp/kafka-server.p12";
    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(4);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(8);

    private static Path tlsDirectory;
    private static Path clientTruststore;
    private static Path wrongTruststore;
    private static SslKafkaContainer kafka;

    @BeforeAll
    static void startTlsBroker() throws Exception {
        tlsDirectory = Files.createTempDirectory("product-search-kafka-tls-");
        Path serverKeystore = tlsDirectory.resolve("server.p12");
        clientTruststore = tlsDirectory.resolve("client-truststore.p12");
        wrongTruststore = tlsDirectory.resolve("wrong-truststore.p12");
        Path serverCertificate = tlsDirectory.resolve("server.cer");
        Path wrongCertificate = tlsDirectory.resolve("wrong.cer");
        Path wrongCertificateKeystore = tlsDirectory.resolve("wrong.p12");

        runKeytool(
                "-genkeypair",
                "-alias", "kafka",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "2",
                "-dname", "CN=localhost",
                "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-storetype", "PKCS12",
                "-keystore", serverKeystore.toString(),
                "-storepass", TLS_PASSWORD,
                "-keypass", TLS_PASSWORD,
                "-noprompt"
        );
        runKeytool(
                "-exportcert",
                "-alias", "kafka",
                "-keystore", serverKeystore.toString(),
                "-storepass", TLS_PASSWORD,
                "-file", serverCertificate.toString()
        );
        runKeytool(
                "-importcert",
                "-alias", "kafka",
                "-file", serverCertificate.toString(),
                "-storetype", "PKCS12",
                "-keystore", clientTruststore.toString(),
                "-storepass", TLS_PASSWORD,
                "-noprompt"
        );
        runKeytool(
                "-genkeypair",
                "-alias", "wrong",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "2",
                "-dname", "CN=wrong",
                "-storetype", "PKCS12",
                "-keystore", wrongCertificateKeystore.toString(),
                "-storepass", TLS_PASSWORD,
                "-keypass", TLS_PASSWORD,
                "-noprompt"
        );
        runKeytool(
                "-exportcert",
                "-alias", "wrong",
                "-keystore", wrongCertificateKeystore.toString(),
                "-storepass", TLS_PASSWORD,
                "-file", wrongCertificate.toString()
        );
        runKeytool(
                "-importcert",
                "-alias", "wrong",
                "-file", wrongCertificate.toString(),
                "-storetype", "PKCS12",
                "-keystore", wrongTruststore.toString(),
                "-storepass", TLS_PASSWORD,
                "-noprompt"
        );

        kafka = new SslKafkaContainer(serverKeystore);
        kafka.start();
    }

    @AfterAll
    static void stopTlsBroker() throws IOException {
        if (kafka != null) {
            kafka.stop();
        }
        if (tlsDirectory != null) {
            try (var paths = Files.walk(tlsDirectory)) {
                for (Path path : paths.sorted((left, right) ->
                        right.getNameCount() - left.getNameCount()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    @Test
    void producerMainConsumerAndDlqConsumerCompleteTlsHandshakes() {
        contextRunner(clientTruststore, true).run(context -> {
            assertTlsProperties(context);
            KafkaTemplate<String, Object> template = context.getBean(
                    "productEventKafkaTemplate",
                    KafkaTemplate.class
            );
            ProductEvent event = ProductEvent.builder()
                    .schemaVersion(1)
                    .eventId(UUID.randomUUID())
                    .version(1L)
                    .productId(1L)
                    .sku("TLS-TEST")
                    .name("TLS Test Product")
                    .price(new BigDecimal("4.25"))
                    .stockQuantity(2)
                    .eventType(ProductEvent.EventType.CREATED)
                    .timestamp(LocalDateTime.now().withNano(0))
                    .build();

            assertThatCode(() -> template.send(
                            new ProducerRecord<>(PRODUCT_TOPIC, event.sku(), event)
                    ).get(CLIENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .as("producer sends an event over TLS")
                    .doesNotThrowAnyException();
            System.out.println("TLS_HANDSHAKE_SUCCESS client=PRODUCER");

            var mainConsumerFactory = context.getBean(
                    "productEventConsumerFactory",
                    ConsumerFactory.class
            );
            try (var consumer = mainConsumerFactory.createConsumer(
                    "tls-main-" + UUID.randomUUID(),
                    "tls-main",
                    ""
            )) {
                consumer.subscribe(List.of(PRODUCT_TOPIC));
                ConsumerRecord<?, ?> received = pollForRecord(consumer, PRODUCT_TOPIC);
                assertThat(received.value()).isEqualTo(event);
            }
            System.out.println("TLS_HANDSHAKE_SUCCESS client=MAIN_CONSUMER");

            assertThatCode(() -> template.send(
                            new ProducerRecord<>(DLT_TOPIC, "tls-dlt", new byte[] {1, 2, 3})
                    ).get(CLIENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .as("setup record for the DLT consumer")
                    .doesNotThrowAnyException();

            var dltContainerFactory = context.getBean(
                    "deadLetterKafkaListenerContainerFactory",
                    ConcurrentKafkaListenerContainerFactory.class
            );
            var dltConsumerFactory = (DefaultKafkaConsumerFactory<?, ?>)
                    dltContainerFactory.getConsumerFactory();
            try (var consumer = dltConsumerFactory.createConsumer(
                    "tls-dlt-" + UUID.randomUUID(),
                    "tls-dlt",
                    ""
            )) {
                consumer.subscribe(List.of(DLT_TOPIC));
                ConsumerRecord<?, ?> received = pollForRecord(consumer, DLT_TOPIC);
                assertThat(received.value()).isEqualTo(new byte[] {1, 2, 3});
            }
            System.out.println("TLS_HANDSHAKE_SUCCESS client=DLT_CONSUMER");
        });
    }

    @Test
    void allThreeClientsFailAgainstSslListenerWhenSecurityProtocolIsUnset() {
        assertClientsFailToConnect(null, FailureKind.PLAINTEXT_TO_SSL);
    }

    @Test
    void allThreeClientsFailWithUntrustedBrokerCertificate() {
        assertClientsFailToConnect(wrongTruststore, FailureKind.UNTRUSTED_CERTIFICATE);
    }

    private static void assertClientsFailToConnect(Path truststore, FailureKind failureKind) {
        for (ClientKind clientKind : ClientKind.values()) {
            contextRunner(truststore == null ? clientTruststore : truststore, truststore != null)
                    .run(context -> assertClientFails(context, clientKind, failureKind));
        }
    }

    private static void assertClientFails(
            ApplicationContext context,
            ClientKind clientKind,
            FailureKind failureKind
    ) {
        Throwable failure;
        try {
            switch (clientKind) {
                case PRODUCER -> sendRecord(context);
                case MAIN_CONSUMER -> listTopics(mainConsumerFactory(context));
                case DLT_CONSUMER -> listTopics(deadLetterConsumerFactory(context));
            }
            throw new AssertionError(
                    clientKind + " unexpectedly connected in " + failureKind + " control"
            );
        } catch (AssertionError assertionError) {
            throw assertionError;
        } catch (Throwable expected) {
            failure = expected;
        }

        String rootMessage = rootCause(failure).getClass().getSimpleName()
                + ": " + rootCause(failure).getMessage();
        System.out.printf(
                "EXPECTED_CONNECTION_FAILURE control=%s client=%s cause=%s%n",
                failureKind,
                clientKind,
                rootMessage
        );
        assertThat(rootMessage)
                .as("expected connection/handshake failure for %s in %s", clientKind, failureKind)
                .isNotBlank();
    }

    private static void sendRecord(ApplicationContext context) throws Exception {
        KafkaTemplate<String, Object> template = context.getBean(
                "productEventKafkaTemplate",
                KafkaTemplate.class
        );
        template.send(new ProducerRecord<>(PRODUCT_TOPIC, "negative-control", "test"))
                .get(CLIENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static void listTopics(ConsumerFactory<?, ?> factory) {
        try (var consumer = factory.createConsumer()) {
            consumer.listTopics(CLIENT_TIMEOUT);
        }
    }

    private static DefaultKafkaConsumerFactory<?, ?> mainConsumerFactory(
            ApplicationContext context
    ) {
        return (DefaultKafkaConsumerFactory<?, ?>) context.getBean(
                "productEventConsumerFactory"
        );
    }

    private static DefaultKafkaConsumerFactory<?, ?> deadLetterConsumerFactory(
            ApplicationContext context
    ) {
        var factory = (ConcurrentKafkaListenerContainerFactory<?, ?>) context.getBean(
                "deadLetterKafkaListenerContainerFactory"
        );
        return (DefaultKafkaConsumerFactory<?, ?>) factory.getConsumerFactory();
    }

    private static ConsumerRecord<?, ?> pollForRecord(
            org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
            String topic
    ) {
        long deadline = System.nanoTime() + POLL_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            var records = consumer.poll(Duration.ofMillis(500));
            var topicRecords = records.records(topic);
            var iterator = topicRecords.iterator();
            if (iterator.hasNext()) {
                return iterator.next();
            }
        }
        throw new AssertionError("No record received from " + topic + " within " + POLL_TIMEOUT);
    }

    private static ApplicationContextRunner contextRunner(Path truststore, boolean sslEnabled) {
        ApplicationContextRunner runner = new ApplicationContextRunner()
                .withUserConfiguration(
                        KafkaProducerConfig.class,
                        KafkaConsumerConfig.class,
                        KafkaPropertiesTestConfiguration.class
                )
                .withPropertyValues(
                        "spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                        "spring.kafka.consumer.group-id=tls-test-" + UUID.randomUUID(),
                        "spring.kafka.consumer.auto-offset-reset=earliest",
                        "spring.kafka.properties[request.timeout.ms]=2000",
                        "spring.kafka.properties[default.api.timeout.ms]=4000",
                        "spring.kafka.properties[socket.connection.setup.timeout.ms]=1500",
                        "spring.kafka.properties[max.block.ms]=5000",
                        "spring.kafka.properties[delivery.timeout.ms]=6000",
                        "spring.kafka.properties[ssl.truststore.location]=" + truststore,
                        "spring.kafka.properties[ssl.truststore.password]=" + TLS_PASSWORD,
                        "spring.kafka.properties[ssl.truststore.type]=PKCS12"
                )
                .withInitializer(context -> installSystemEnvironment(
                        context,
                        sslEnabled
                ));
        return runner;
    }

    private static void installSystemEnvironment(
            ConfigurableApplicationContext context,
            boolean sslEnabled
    ) {
        ConfigurableEnvironment environment = context.getEnvironment();
        Map<String, Object> variables = new HashMap<>();
        if (sslEnabled) {
            variables.put("SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL", "SSL");
        }
        environment.getPropertySources().replace(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                new SystemEnvironmentPropertySource(
                        StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                        variables
                )
        );
        ConfigurationPropertySources.attach(environment);
    }

    private static void assertTlsProperties(ApplicationContext context) {
        assertThat(context.getBean(KafkaProperties.class).getProperties())
                .containsEntry("security.protocol", "SSL");
        assertThat(producerFactory(context).getConfigurationProperties())
                .containsEntry("security.protocol", "SSL");
        assertThat(mainConsumerFactory(context).getConfigurationProperties())
                .containsEntry("security.protocol", "SSL");
        assertThat(deadLetterConsumerFactory(context).getConfigurationProperties())
                .containsEntry("security.protocol", "SSL");
    }

    private static DefaultKafkaProducerFactory<?, ?> producerFactory(
            ApplicationContext context
    ) {
        return (DefaultKafkaProducerFactory<?, ?>) context.getBean(
                "productEventProducerFactory"
        );
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static void runKeytool(String... arguments) throws Exception {
        Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
        Process process = new ProcessBuilder()
                .command(joinCommand(keytool.toString(), arguments))
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("keytool command timed out");
        }
        assertThat(process.exitValue())
                .as("keytool command %s", arguments[0])
                .isZero();
    }

    private static List<String> joinCommand(String executable, String... arguments) {
        var command = new java.util.ArrayList<String>();
        command.add(executable);
        command.addAll(List.of(arguments));
        return command;
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(KafkaProperties.class)
    static class KafkaPropertiesTestConfiguration {
    }

    private enum ClientKind {
        PRODUCER,
        MAIN_CONSUMER,
        DLT_CONSUMER
    }

    private enum FailureKind {
        PLAINTEXT_TO_SSL,
        UNTRUSTED_CERTIFICATE
    }

    private static final class SslKafkaContainer extends ConfluentKafkaContainer {

        private SslKafkaContainer(Path serverKeystore) {
            super(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
            withCopyFileToContainer(
                    org.testcontainers.utility.MountableFile.forHostPath(serverKeystore),
                    CONTAINER_KEYSTORE
            );
            withEnv("KAFKA_SSL_KEYSTORE_LOCATION", CONTAINER_KEYSTORE);
            withEnv("KAFKA_SSL_KEYSTORE_PASSWORD", TLS_PASSWORD);
            withEnv("KAFKA_SSL_KEY_PASSWORD", TLS_PASSWORD);
            withEnv("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12");
            withEnv("KAFKA_SSL_CLIENT_AUTH", "none");
        }

        @Override
        protected void configure() {
            super.configure();
            String listenerProtocols = getEnvMap().get("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP");
            if (listenerProtocols == null || !listenerProtocols.contains("PLAINTEXT:PLAINTEXT")) {
                throw new IllegalStateException(
                        "Expected Testcontainers to configure its external PLAINTEXT listener"
                );
            }
            getEnvMap().put(
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                    listenerProtocols.replace("PLAINTEXT:PLAINTEXT", "PLAINTEXT:SSL")
            );
        }
    }
}
