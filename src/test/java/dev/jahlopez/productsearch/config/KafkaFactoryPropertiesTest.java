package dev.jahlopez.productsearch.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaFactoryPropertiesTest {

    private static final String EXPLICIT_BOOTSTRAP_SERVERS = "explicit-broker:9092";
    private static final String EXPLICIT_GROUP_ID = "explicit-group";
    private static final String EXPLICIT_AUTO_OFFSET_RESET = "earliest";

    @Test
    void copiesKafkaPropertiesFromRegularPropertySource() {
        contextRunner()
                .withPropertyValues("spring.kafka.properties.security.protocol=SSL")
                .run(context -> assertFactoryProperties(context));
    }

    @Test
    void copiesKafkaPropertiesFromSystemEnvironmentPropertySource() {
        contextRunner()
                .withInitializer(context -> replaceSystemEnvironment(context.getEnvironment()))
                .run(context -> {
            assertThat(context.getEnvironment().getProperty(
                    "spring.kafka.properties.security.protocol"
            )).isEqualTo("SSL");
            assertFactoryProperties(context);
        });
    }

    private static ApplicationContextRunner contextRunner() {
        return new ApplicationContextRunner()
                .withUserConfiguration(
                        KafkaProducerConfig.class,
                        KafkaConsumerConfig.class,
                        KafkaPropertiesTestConfiguration.class
                )
                .withPropertyValues(
                        "spring.kafka.bootstrap-servers=" + EXPLICIT_BOOTSTRAP_SERVERS,
                        "spring.kafka.consumer.group-id=" + EXPLICIT_GROUP_ID,
                        "spring.kafka.consumer.auto-offset-reset=" + EXPLICIT_AUTO_OFFSET_RESET,
                        "spring.kafka.properties[bootstrap.servers]=seeded-broker:9093",
                        "spring.kafka.properties[group.id]=seeded-group",
                        "spring.kafka.properties[auto.offset.reset]=latest"
                );
    }

    private static void replaceSystemEnvironment(ConfigurableEnvironment environment) {
        Map<String, Object> environmentVariables = new HashMap<>();
        environmentVariables.put("SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL", "SSL");
        environment.getPropertySources().replace(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                new SystemEnvironmentPropertySource(
                        StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                        environmentVariables
                )
        );
        ConfigurationPropertySources.attach(environment);
    }

    private static void assertFactoryProperties(
            ApplicationContext context
    ) {
        assertThat(context.getBean(KafkaProperties.class).getProperties())
                .containsEntry("security.protocol", "SSL");
        assertThat(producerFactory(context).getConfigurationProperties())
                .containsEntry("security.protocol", "SSL");
        assertThat(mainConsumerFactory(context).getConfigurationProperties())
                .containsEntry("security.protocol", "SSL");
        assertThat(deadLetterConsumerFactory(context).getConfigurationProperties())
                .containsEntry("security.protocol", "SSL");
        assertExplicitProperties(context);
    }

    private static void assertExplicitProperties(
            ApplicationContext context
    ) {
        assertThat(producerFactory(context).getConfigurationProperties())
                .containsEntry(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        EXPLICIT_BOOTSTRAP_SERVERS
                );
        assertThat(mainConsumerFactory(context).getConfigurationProperties())
                .containsEntry(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        EXPLICIT_BOOTSTRAP_SERVERS
                )
                .containsEntry(ConsumerConfig.GROUP_ID_CONFIG, EXPLICIT_GROUP_ID)
                .containsEntry(
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        EXPLICIT_AUTO_OFFSET_RESET
                );
        assertThat(deadLetterConsumerFactory(context).getConfigurationProperties())
                .containsEntry(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        EXPLICIT_BOOTSTRAP_SERVERS
                )
                .containsEntry(
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        EXPLICIT_AUTO_OFFSET_RESET
                );
    }

    private static DefaultKafkaProducerFactory<?, ?> producerFactory(
            ApplicationContext context
    ) {
        return (DefaultKafkaProducerFactory<?, ?>) context.getBean(
                "productEventProducerFactory"
        );
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

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(KafkaProperties.class)
    static class KafkaPropertiesTestConfiguration {
    }
}
