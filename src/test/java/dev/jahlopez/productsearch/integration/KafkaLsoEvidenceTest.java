package dev.jahlopez.productsearch.integration;

import dev.jahlopez.productsearch.config.KafkaConsumerConfig;
import dev.jahlopez.productsearch.config.KafkaProducerConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaLsoEvidenceTest {

    private static final int PARTITION_COUNT = 3;
    private static final short REPLICATION_FACTOR = 1;
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);
    private static final Duration REPLAY_TIMEOUT = Duration.ofSeconds(8);

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.7.0")
    );

    @Test
    void s1NonTransactionalRecordsHaveEqualLsoAndHighWatermark() throws Exception {
        String topic = newTopic("s1");
        try (var producer = plainProducer()) {
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                send(producer, topic, partition, "s1-p" + partition);
            }
        }

        OffsetCapture capture = capture(topic, "S1");
        assertThat(capture.readCommitted()).isEqualTo(capture.readUncommitted());
        System.out.printf("LSO_EVIDENCE scenario=S1 expected=LSO_equals_HW observed=%s%n",
                capture.readCommitted());
    }

    @Test
    void s2OpenTransactionHoldsLsoAndReplayFromItReceivesAfterCommit() throws Exception {
        String topic = newTopic("s2");
        seedPartitions(topic, "s2-seed");
        Map<Integer, Long> firstTransactionOffsets;
        try (var producer = transactionalProducer("s2-" + UUID.randomUUID())) {
            producer.beginTransaction();
            firstTransactionOffsets = sendToPartitions(
                    producer,
                    topic,
                    Map.of(0, "s2-tx-p0", 1, "s2-tx-p1")
            );

            OffsetCapture capture = capture(topic, "S2");
            assertOpenTransactionBoundary(capture, firstTransactionOffsets, Set.of(0, 1));

            producer.commitTransaction();
            List<ConsumerRecord<String, String>> replay = consumeFrom(
                    topic,
                    capture.readCommitted(),
                    "read_committed",
                    2
            );
            assertReplay(replay, Set.of("s2-tx-p0", "s2-tx-p1"), capture.readCommitted());
        }
    }

    @Test
    void s3AbortedTransactionAdvancesLsoAndIsNeverDeliveredReadCommitted() throws Exception {
        String topic = newTopic("s3");
        Map<Integer, Long> abortedOffsets;
        try (var producer = transactionalProducer("s3-" + UUID.randomUUID())) {
            producer.beginTransaction();
            abortedOffsets = sendToPartitions(
                    producer,
                    topic,
                    Map.of(0, "s3-aborted-p0", 1, "s3-aborted-p1")
            );
            producer.abortTransaction();
        }

        OffsetCapture capture = capture(topic, "S3");
        for (var entry : abortedOffsets.entrySet()) {
            assertThat(capture.readCommitted().get(entry.getKey()))
                    .as("LSO advances beyond aborted offset in partition %s", entry.getKey())
                    .isGreaterThan(entry.getValue());
        }

        List<ConsumerRecord<String, String>> fromCapturedVector = consumeFrom(
                topic,
                capture.readCommitted(),
                "read_committed",
                0
        );
        List<ConsumerRecord<String, String>> fromEarliest = consumeFromBeginning(
                topic,
                "read_committed"
        );
        assertThat(fromCapturedVector).isEmpty();
        assertThat(fromEarliest).isEmpty();
        System.out.printf(
                "LSO_EVIDENCE scenario=S3 aborted_offsets=%s LSO_advanced_past_aborted=true delivered_from_E=0 delivered_from_earliest=0%n",
                abortedOffsets
        );
    }

    @Test
    void s4PlainRecordsAfterOpenTransactionRemainAtOrBeyondCapturedLso() throws Exception {
        String topic = newTopic("s4");
        seedPartitions(topic, "s4-seed");
        Map<Integer, Long> firstTransactionOffsets;
        RecordMetadata laterPlainRecord;
        try (var transactionalProducer =
                     transactionalProducer("s4-" + UUID.randomUUID());
             var plainProducer = plainProducer()) {
            transactionalProducer.beginTransaction();
            firstTransactionOffsets = sendToPartitions(
                    transactionalProducer,
                    topic,
                    Map.of(0, "s4-tx-p0")
            );
            laterPlainRecord = send(plainProducer, topic, 0, "s4-plain-after-open-tx");

            OffsetCapture capture = capture(topic, "S4");
            assertOpenTransactionBoundary(capture, firstTransactionOffsets, Set.of(0));
            assertThat(capture.readCommitted().get(0))
                    .as("captured LSO is below a later non-transactional record")
                    .isLessThan(laterPlainRecord.offset());

            transactionalProducer.commitTransaction();
            List<ConsumerRecord<String, String>> replay = consumeFrom(
                    topic,
                    capture.readCommitted(),
                    "read_committed",
                    2
            );
            assertReplay(
                    replay,
                    Set.of("s4-tx-p0", "s4-plain-after-open-tx"),
                    capture.readCommitted()
            );
            assertThat(laterPlainRecord.offset()).isGreaterThanOrEqualTo(
                    capture.readCommitted().get(0)
            );
            System.out.printf(
                    "LSO_EVIDENCE scenario=S4 later_plain_offset=%d E_p0=%d later_plain_offset_gte_E=true%n",
                    laterPlainRecord.offset(),
                    capture.readCommitted().get(0)
            );
        }
    }

    @Test
    void s5TransactionAcrossPartitionsCommitsOnlyAtOrBeyondCapturedVector() throws Exception {
        String topic = newTopic("s5");
        seedPartitions(topic, "s5-seed");
        Map<Integer, Long> transactionOffsets;
        try (var producer = transactionalProducer("s5-" + UUID.randomUUID())) {
            producer.beginTransaction();
            transactionOffsets = sendToPartitions(
                    producer,
                    topic,
                    Map.of(
                            0, "s5-tx-p0",
                            1, "s5-tx-p1",
                            2, "s5-tx-p2"
                    )
            );
            OffsetCapture capture = capture(topic, "S5");
            assertOpenTransactionBoundary(
                    capture,
                    transactionOffsets,
                    Set.of(0, 1, 2)
            );

            producer.commitTransaction();
            List<ConsumerRecord<String, String>> replay = consumeFrom(
                    topic,
                    capture.readCommitted(),
                    "read_committed",
                    3
            );
            assertReplay(
                    replay,
                    Set.of("s5-tx-p0", "s5-tx-p1", "s5-tx-p2"),
                    capture.readCommitted()
            );
        }
        System.out.println(
                "LSO_EVIDENCE scenario=S5 commit_marker_midflight=not_run reason=no_deterministic_client_side_commit_marker_pause"
        );
    }

    @Test
    void s6ReplayFromCapturedVectorsReturnsExactlyCommittedRecordsAtOrBeyondE()
            throws Exception {
        replayOpenTransactionVector("S6-S2", "s6-s2", Map.of(0, "s6-s2-p0", 1, "s6-s2-p1"));
        replayOpenTransactionAndLaterPlainVector();
        replaySpanningTransactionVector();
    }

    @Test
    void s7DefaultIsolationMatchesReadUncommittedInsteadOfReadCommitted() throws Exception {
        String topic = newTopic("s7");
        seedPartitions(topic, "s7-seed");
        try (var producer = transactionalProducer("s7-" + UUID.randomUUID())) {
            producer.beginTransaction();
            Map<Integer, Long> txOffsets = sendToPartitions(
                    producer,
                    topic,
                    Map.of(0, "s7-tx-p0", 1, "s7-tx-p1")
            );

            Map<Integer, Long> readCommitted = endOffsets(topic, "read_committed");
            Map<Integer, Long> readUncommitted = endOffsets(topic, "read_uncommitted");
            Map<Integer, Long> defaultIsolation = endOffsets(topic, null);
            printVector("S7", "read_committed", readCommitted, readUncommitted);
            printVector("S7", "read_uncommitted", readUncommitted, readUncommitted);
            printVector("S7", "default", defaultIsolation, readUncommitted);

            assertOpenTransactionBoundary(
                    new OffsetCapture(readCommitted, readUncommitted),
                    txOffsets,
                    Set.of(0, 1)
            );
            assertThat(defaultIsolation).isEqualTo(readUncommitted);
            assertThat(defaultIsolation).isNotEqualTo(readCommitted);
            producer.abortTransaction();
        }
    }

    @Test
    void s8ApplicationMainConsumerDefaultsToReadUncommitted() {
        applicationContextRunner().run(context -> {
            DefaultKafkaConsumerFactory<?, ?> factory = mainConsumerFactory(context);
            Map<String, Object> properties =
                    new HashMap<>(factory.getConfigurationProperties());
            properties.put(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    factory.getKeyDeserializer().getClass()
            );
            properties.put(
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    factory.getValueDeserializer().getClass()
            );
            var effectiveConfig = new ConsumerConfig(properties);
            String isolation = effectiveConfig.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG);

            System.out.printf(
                    "LSO_EVIDENCE scenario=S8 application_main_consumer_isolation=%s%n",
                    isolation
            );
            assertThat(isolation).isEqualTo("read_uncommitted");
        });
    }

    private static void replayOpenTransactionVector(
            String scenario,
            String topicPrefix,
            Map<Integer, String> transactionValues
    ) throws Exception {
        String topic = newTopic(topicPrefix);
        seedPartitions(topic, topicPrefix + "-seed");
        try (var producer = transactionalProducer(topicPrefix + "-" + UUID.randomUUID())) {
            producer.beginTransaction();
            Map<Integer, Long> offsets = sendToPartitions(producer, topic, transactionValues);
            OffsetCapture capture = capture(topic, scenario);
            assertOpenTransactionBoundary(
                    capture,
                    offsets,
                    transactionValues.keySet()
            );
            producer.commitTransaction();
            List<ConsumerRecord<String, String>> replay = consumeFrom(
                    topic,
                    capture.readCommitted(),
                    "read_committed",
                    transactionValues.size()
            );
            assertReplay(
                    replay,
                    new HashSet<>(transactionValues.values()),
                    capture.readCommitted()
            );
        }
    }

    private static void replayOpenTransactionAndLaterPlainVector() throws Exception {
        String topic = newTopic("s6-s4");
        seedPartitions(topic, "s6-s4-seed");
        try (var transaction = transactionalProducer("s6-s4-" + UUID.randomUUID());
             var plain = plainProducer()) {
            transaction.beginTransaction();
            Map<Integer, Long> offsets = sendToPartitions(
                    transaction,
                    topic,
                    Map.of(0, "s6-s4-tx")
            );
            RecordMetadata plainRecord = send(plain, topic, 0, "s6-s4-plain-after");
            OffsetCapture capture = capture(topic, "S6-S4");
            assertOpenTransactionBoundary(capture, offsets, Set.of(0));
            assertThat(plainRecord.offset()).isGreaterThanOrEqualTo(
                    capture.readCommitted().get(0)
            );
            transaction.commitTransaction();
            List<ConsumerRecord<String, String>> replay = consumeFrom(
                    topic,
                    capture.readCommitted(),
                    "read_committed",
                    2
            );
            assertReplay(
                    replay,
                    Set.of("s6-s4-tx", "s6-s4-plain-after"),
                    capture.readCommitted()
            );
        }
    }

    private static void replaySpanningTransactionVector() throws Exception {
        String topic = newTopic("s6-s5");
        seedPartitions(topic, "s6-s5-seed");
        Map<Integer, String> values = Map.of(
                0, "s6-s5-p0",
                1, "s6-s5-p1",
                2, "s6-s5-p2"
        );
        try (var producer = transactionalProducer("s6-s5-" + UUID.randomUUID())) {
            producer.beginTransaction();
            Map<Integer, Long> offsets = sendToPartitions(producer, topic, values);
            OffsetCapture capture = capture(topic, "S6-S5");
            assertOpenTransactionBoundary(capture, offsets, Set.of(0, 1, 2));
            producer.commitTransaction();
            List<ConsumerRecord<String, String>> replay = consumeFrom(
                    topic,
                    capture.readCommitted(),
                    "read_committed",
                    values.size()
            );
            assertReplay(
                    replay,
                    new HashSet<>(values.values()),
                    capture.readCommitted()
            );
        }
    }

    private static OffsetCapture capture(String topic, String scenario) {
        Map<Integer, Long> readCommitted = endOffsets(topic, "read_committed");
        Map<Integer, Long> readUncommitted = endOffsets(topic, "read_uncommitted");
        printVector(scenario, "read_committed", readCommitted, readUncommitted);
        printVector(scenario, "read_uncommitted", readUncommitted, readUncommitted);
        return new OffsetCapture(readCommitted, readUncommitted);
    }

    private static Map<Integer, Long> endOffsets(String topic, String isolationLevel) {
        Map<String, Object> properties = consumerProperties(topic, isolationLevel);
        try (var consumer = new KafkaConsumer<String, String>(
                properties,
                new StringDeserializer(),
                new StringDeserializer()
        )) {
            List<TopicPartition> partitions = topicPartitions(topic);
            consumer.assign(partitions);
            Map<TopicPartition, Long> endOffsets =
                    consumer.endOffsets(partitions, REQUEST_TIMEOUT);
            Map<Integer, Long> result = new HashMap<>();
            endOffsets.forEach((partition, offset) ->
                    result.put(partition.partition(), offset)
            );
            return ordered(result);
        }
    }

    private static void printVector(
            String scenario,
            String isolation,
            Map<Integer, Long> vector,
            Map<Integer, Long> highWatermark
    ) {
        System.out.printf(
                "LSO_EVIDENCE scenario=%s isolation=%s E=%s HW=%s%n",
                scenario,
                isolation,
                vector,
                highWatermark
        );
    }

    private static void assertOpenTransactionBoundary(
            OffsetCapture capture,
            Map<Integer, Long> firstTransactionOffsets,
            Set<Integer> touchedPartitions
    ) {
        for (int partition : touchedPartitions) {
            assertThat(capture.readCommitted().get(partition))
                    .as("LSO is at or before first open transaction record, partition %s", partition)
                    .isLessThanOrEqualTo(firstTransactionOffsets.get(partition));
            assertThat(capture.readUncommitted().get(partition))
                    .as("high watermark is beyond first open transaction record, partition %s", partition)
                    .isGreaterThan(firstTransactionOffsets.get(partition));
        }
        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            if (!touchedPartitions.contains(partition)) {
                assertThat(capture.readCommitted().get(partition))
                        .isEqualTo(capture.readUncommitted().get(partition));
            }
        }
    }

    private static void assertReplay(
            List<ConsumerRecord<String, String>> records,
            Set<String> expectedValues,
            Map<Integer, Long> capturedOffsets
    ) {
        Set<String> actualValues = new HashSet<>();
        for (ConsumerRecord<String, String> record : records) {
            actualValues.add(record.value());
            assertThat(record.offset())
                    .as("replayed record offset is at or beyond captured E[p]")
                    .isGreaterThanOrEqualTo(capturedOffsets.get(record.partition()));
        }
        assertThat(actualValues).isEqualTo(expectedValues);
        assertThat(records).hasSize(expectedValues.size());
    }

    private static List<ConsumerRecord<String, String>> consumeFrom(
            String topic,
            Map<Integer, Long> offsets,
            String isolationLevel,
            int expectedCount
    ) {
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProperties(topic, isolationLevel),
                new StringDeserializer(),
                new StringDeserializer()
        )) {
            List<TopicPartition> partitions = topicPartitions(topic);
            consumer.assign(partitions);
            partitions.forEach(partition ->
                    consumer.seek(partition, offsets.get(partition.partition()))
            );
            return pollExpected(consumer, topic, expectedCount, REPLAY_TIMEOUT);
        }
    }

    private static List<ConsumerRecord<String, String>> consumeFromBeginning(
            String topic,
            String isolationLevel
    ) {
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProperties(topic, isolationLevel),
                new StringDeserializer(),
                new StringDeserializer()
        )) {
            List<TopicPartition> partitions = topicPartitions(topic);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            return pollExpected(consumer, topic, 0, Duration.ofSeconds(2));
        }
    }

    private static List<ConsumerRecord<String, String>> pollExpected(
            Consumer<String, String> consumer,
            String topic,
            int expectedCount,
            Duration timeout
    ) {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        while (System.nanoTime() < deadline
                && (expectedCount == 0 || records.size() < expectedCount)) {
            ConsumerRecords<String, String> polled = consumer.poll(POLL_INTERVAL);
            polled.records(topic).forEach(records::add);
        }
        return records;
    }

    private static Map<Integer, Long> sendToPartitions(
            KafkaProducer<String, String> producer,
            String topic,
            Map<Integer, String> values
    ) throws Exception {
        Map<Integer, Long> offsets = new HashMap<>();
        for (var entry : values.entrySet()) {
            RecordMetadata metadata = send(producer, topic, entry.getKey(), entry.getValue());
            offsets.put(entry.getKey(), metadata.offset());
        }
        return ordered(offsets);
    }

    private static RecordMetadata send(
            KafkaProducer<String, String> producer,
            String topic,
            int partition,
            String value
    ) throws Exception {
        return producer.send(new ProducerRecord<>(
                        topic,
                        partition,
                        "key-" + value,
                        value
                ))
                .get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static void seedPartitions(String topic, String prefix) throws Exception {
        try (var producer = plainProducer()) {
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                send(producer, topic, partition, prefix + "-p" + partition);
            }
        }
    }

    private static KafkaProducer<String, String> plainProducer() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 8_000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        return new KafkaProducer<>(properties);
    }

    private static KafkaProducer<String, String> transactionalProducer(String transactionalId) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 8_000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        producer.initTransactions();
        return producer;
    }

    private static Map<String, Object> consumerProperties(
            String topic,
            String isolationLevel
    ) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "lso-" + topic + "-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 2_000);
        if (isolationLevel != null) {
            properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        }
        return properties;
    }

    private static String newTopic(String prefix) throws Exception {
        String topic = "lso-" + prefix + "-" + UUID.randomUUID();
        try (var admin = AdminClient.create(Map.of(
                "bootstrap.servers",
                kafka.getBootstrapServers(),
                "request.timeout.ms",
                5_000,
                "default.api.timeout.ms",
                5_000
        ))) {
            admin.createTopics(List.of(new NewTopic(
                    topic,
                    PARTITION_COUNT,
                    REPLICATION_FACTOR
            ))).all().get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
        return topic;
    }

    private static List<TopicPartition> topicPartitions(String topic) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            partitions.add(new TopicPartition(topic, partition));
        }
        return partitions;
    }

    private static Map<Integer, Long> ordered(Map<Integer, Long> offsets) {
        Map<Integer, Long> result = new java.util.TreeMap<>();
        result.putAll(offsets);
        return result;
    }

    private static ApplicationContextRunner applicationContextRunner() {
        return new ApplicationContextRunner()
                .withUserConfiguration(
                        KafkaProducerConfig.class,
                        KafkaConsumerConfig.class,
                        KafkaPropertiesTestConfiguration.class
                )
                .withPropertyValues(
                        "spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                        "spring.kafka.consumer.group-id=lso-app-consumer",
                        "spring.kafka.consumer.auto-offset-reset=earliest"
                );
    }

    private static DefaultKafkaConsumerFactory<?, ?> mainConsumerFactory(
            ApplicationContext context
    ) {
        return (DefaultKafkaConsumerFactory<?, ?>) context.getBean(
                "productEventConsumerFactory"
        );
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(KafkaProperties.class)
    static class KafkaPropertiesTestConfiguration {
    }

    private record OffsetCapture(
            Map<Integer, Long> readCommitted,
            Map<Integer, Long> readUncommitted
    ) {
    }
}
