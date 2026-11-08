package dev.jahlopez.productsearch.integration;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.DockerClientFactory;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DebeziumOutboxEvidenceTest {
    private static final String TOPIC = "product-events";
    private static final String CONNECTOR_NAME = "product-outbox-connector";
    private static final String SLOT_NAME = "debezium_product_outbox_slot";
    private static final int PARTITIONS = 3;
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration CONNECTOR_TIMEOUT = Duration.ofSeconds(45);
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final TypeReference<Map<String, Object>> JSON_MAP = new TypeReference<>() {
    };
    private static final ImageFromDockerfile CONNECT_IMAGE =
            new ImageFromDockerfile("debz-outbox-evidence-connect-7.6.14-db-2.5.4", false)
                    .withDockerfileFromBuilder(builder -> builder
                            .from("confluentinc/cp-kafka-connect:7.6.14")
                            .run("confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4")
                            .build());

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private Network network;
    private KafkaContainer kafka;
    private PostgreSQLContainer<?> postgres;
    private GenericContainer<?> connect;
    private ConnectorDefinition connectorDefinition;

    @BeforeEach
    void startEnvironment() throws Exception {
        network = Network.newNetwork();

        kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withListener("kafka:19092")
                .withStartupTimeout(Duration.ofSeconds(90));
        kafka.start();

        postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15.4"))
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withDatabaseName("product_catalog")
                .withUsername("postgres")
                .withPassword("evidence-only-password")
                .withCommand("postgres", "-c", "wal_level=logical",
                        "-c", "max_replication_slots=10", "-c", "max_wal_senders=10")
                .withStartupTimeout(Duration.ofSeconds(60));
        postgres.start();

        runCatalogMigrations();
        createPublication();
        createOutboxTopic();

        connectorDefinition = readConnectorDefinition();
        connect = newConnectContainer();
        connect.start();
        awaitConnectRest();
        registerConnector(CONNECTOR_NAME);
        awaitConnectorState(CONNECTOR_NAME, "RUNNING", CONNECTOR_TIMEOUT);
    }

    @AfterEach
    void stopEnvironment() {
        if (connect != null) {
            connect.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @Test
    void c1BaselinePublication() throws Exception {
        List<Event> expected = insertEvents("C1", 6, 1);
        List<ObservedRecord> records = awaitTopicRecords(expected.size(), Duration.ofSeconds(30));
        OffsetVectors vectors = captureVectors();
        long offsetGaps = countOffsetGaps(records, vectors.readUncommitted());
        long badKeys = records.stream().filter(record -> !expectedKey(record)).count();
        long badPayloads = records.stream().filter(record -> !expectedPayload(record)).count();

        evidence("C1",
                "expected=" + expected.size()
                        + " observed=" + records.size()
                        + " badKeys=" + badKeys
                        + " badPayloads=" + badPayloads
                        + " offsetGaps=" + offsetGaps
                        + " rc=" + vectors.readCommitted()
                        + " ru=" + vectors.readUncommitted());
        assertEquals(expected.size(), records.size(), "Baseline rows must reach Kafka");
        assertEquals(0, badKeys, "Outbox event keys should be aggregate_id");
        assertEquals(0, badPayloads, "Outbox event payload shape should be preserved");
    }

    @Test
    void c2GracefulConnectorRestart() throws Exception {
        List<Event> beforeRestart = insertEvents("C2-before", 4, 1);
        awaitTopicRecords(beforeRestart.size(), Duration.ofSeconds(30));
        HttpResponse<String> restart = request("POST",
                "/connectors/" + CONNECTOR_NAME + "/restart?includeTasks=true&onlyFailed=false", null);
        assertTrue(restart.statusCode() == 202 || restart.statusCode() == 204,
                "Connect restart should be accepted: " + restart.statusCode());
        awaitConnectorState(CONNECTOR_NAME, "RUNNING", CONNECTOR_TIMEOUT);

        List<Event> afterRestart = insertEvents("C2-after", 5, 1);
        List<ObservedRecord> records = awaitTopicRecords(
                beforeRestart.size() + afterRestart.size(), Duration.ofSeconds(30));
        ReportCounts counts = countExpectedEvents(records, concatenate(beforeRestart, afterRestart));
        evidence("C2",
                "expected=" + counts.expected()
                        + " delivered=" + counts.delivered()
                        + " lost=" + counts.lost()
                        + " duplicates=" + counts.duplicates()
                        + " rc=" + captureVector("read_committed"));
        assertEquals(0, counts.lost(), "Graceful restart must not lose events");
        assertEquals(0, counts.duplicates(), "Graceful restart should not duplicate events");
    }

    @Test
    void c3HardKillDuringSteadyInsertLoad() throws Exception {
        int eventCount = 40;
        AtomicBoolean keepWriting = new AtomicBoolean(true);
        AtomicInteger insertedCount = new AtomicInteger();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        List<Event> expected = Collections.synchronizedList(new ArrayList<>());
        Thread writer = new Thread(() -> {
            try (Connection connection = openDatabase()) {
                for (int i = 0; i < eventCount && keepWriting.get(); i++) {
                    Event event = event("C3-" + (i % 4), (i / 4) + 1);
                    insertEvent(connection, event);
                    expected.add(event);
                    insertedCount.incrementAndGet();
                    Thread.sleep(100);
                }
            } catch (Throwable failure) {
                writerFailure.set(failure);
            }
        }, "debz-evidence-c3-writer");
        writer.start();
        waitUntil(() -> insertedCount.get() >= 5, Duration.ofSeconds(10), "steady insert load");

        killAndRestartConnect();
        Thread.sleep(300);
        keepWriting.set(false);
        writer.join(TimeUnit.SECONDS.toMillis(10));
        assertTrue(!writer.isAlive(), "Insert load must stop within its bound");
        if (writerFailure.get() != null) {
            throw new AssertionError("C3 insert writer failed", writerFailure.get());
        }
        awaitConnectorState(CONNECTOR_NAME, "RUNNING", CONNECTOR_TIMEOUT);

        List<ObservedRecord> records = awaitTopicRecords(expected.size(), Duration.ofSeconds(45));
        ReportCounts counts = countExpectedEvents(records, expected);
        int orderingViolations = countOrderingViolations(records);
        evidence("C3",
                "inserted=" + expected.size()
                        + " delivered=" + counts.delivered()
                        + " lost=" + counts.lost()
                        + " duplicates=" + counts.duplicates()
                        + " orderingViolations=" + orderingViolations
                        + " rc=" + captureVector("read_committed")
                        + " slot=" + slotStats());
        assertEquals(0, counts.lost(), "Hard Connect restart must not lose events");
        assertEquals(0, orderingViolations, "Per-aggregate order must be preserved");
    }

    @Test
    void c4ConnectorOutageCatchupAndWalRetention() throws Exception {
        List<Event> initial = insertEvents("C4-initial", 3, 1);
        awaitTopicRecords(initial.size(), Duration.ofSeconds(30));
        WalStats before = slotStats();
        pauseConnector();
        List<Event> during = insertEvents("C4-outage", 8, 1);
        WalStats duringOutage = slotStats();
        resumeConnector();

        List<ObservedRecord> records = awaitTopicRecords(
                initial.size() + during.size(), Duration.ofSeconds(45));
        WalStats after = slotStats();
        ReportCounts counts = countExpectedEvents(records, concatenate(initial, during));
        evidence("C4",
                "expected=" + counts.expected()
                        + " delivered=" + counts.delivered()
                        + " lost=" + counts.lost()
                        + " duplicates=" + counts.duplicates()
                        + " walBefore=" + before
                        + " walDuring=" + duringOutage
                        + " walAfter=" + after
                        + " rc=" + captureVector("read_committed"));
        assertEquals(0, counts.lost(), "Connector outage must catch up all source events");
    }

    @Test
    void c5OffsetsLostWhileSlotPersists() throws Exception {
        List<Event> initial = insertEvents("C5-before", 3, 1);
        awaitTopicRecords(initial.size(), Duration.ofSeconds(30));
        HttpResponse<String> deleteConnector = request("DELETE", "/connectors/" + CONNECTOR_NAME, null);
        awaitConnectorAbsent(CONNECTOR_NAME);
        boolean slotPersisted = slotExists();
        assertTrue(slotPersisted, "Deleting a connector should leave the replication slot for this test");
        List<Event> afterReset = insertEvents("C5-after", 5, 1);
        connect.stop();
        connect = newConnectContainer("dbz-evidence-offsets-reset");
        connect.start();
        awaitConnectRest();
        registerConnector(CONNECTOR_NAME);

        String state = awaitTerminalConnectorState(CONNECTOR_NAME, Duration.ofSeconds(30));
        if ("RUNNING".equals(state)) {
            awaitTopicRecords(initial.size() + afterReset.size(), Duration.ofSeconds(30));
        }
        List<ObservedRecord> records = readTopic(Duration.ofSeconds(2));
        ReportCounts counts = countExpectedEvents(records, concatenate(initial, afterReset));
        evidence("C5",
                "deleteConnectorStatus=" + deleteConnector.statusCode()
                        + " freshOffsetsTopic=true"
                        + " slotPersisted=" + slotPersisted
                        + " state=" + state
                        + " expected=" + counts.expected()
                        + " delivered=" + counts.delivered()
                        + " lost=" + counts.lost()
                        + " duplicates=" + counts.duplicates()
                        + " slot=" + slotStats()
                        + " rc=" + captureVector("read_committed"));
    }

    @Test
    void c6ReplicationSlotDroppedWhileConnectorDown() throws Exception {
        List<Event> initial = insertEvents("C6-before", 3, 1);
        awaitTopicRecords(initial.size(), Duration.ofSeconds(30));
        HttpResponse<String> deleteConnector = request("DELETE", "/connectors/" + CONNECTOR_NAME, null);
        awaitConnectorAbsent(CONNECTOR_NAME);
        boolean slotPersisted = slotExists();
        assertTrue(slotPersisted, "The slot must exist before testing slot-loss recovery");
        try (Connection connection = openDatabase(); Statement statement = connection.createStatement()) {
            statement.execute("SELECT pg_drop_replication_slot('" + SLOT_NAME + "')");
        }
        List<Event> during = insertEvents("C6-gap", 6, 1);
        registerConnector(CONNECTOR_NAME);
        String state = awaitTerminalConnectorState(CONNECTOR_NAME, Duration.ofSeconds(30));
        if ("RUNNING".equals(state)) {
            awaitTopicRecords(initial.size() + during.size(), Duration.ofSeconds(30));
        }
        List<ObservedRecord> records = readTopic(Duration.ofSeconds(2));
        ReportCounts counts = countExpectedEvents(records, concatenate(initial, during));
        String trace = connectorTrace(CONNECTOR_NAME);
        evidence("C6",
                "deleteConnectorStatus=" + deleteConnector.statusCode()
                        + " slotPersistedBeforeDrop=" + slotPersisted
                        + " state=" + state
                        + " expected=" + counts.expected()
                        + " delivered=" + counts.delivered()
                        + " lost=" + counts.lost()
                        + " duplicates=" + counts.duplicates()
                        + " slot=" + slotStats()
                        + " failure=" + abbreviated(trace));
    }

    @Test
    void c7DeleteAndRecreateConnectorNamesWithSameSlot() throws Exception {
        List<Event> initial = insertEvents("C7-initial", 3, 1);
        awaitTopicRecords(initial.size(), Duration.ofSeconds(30));
        request("DELETE", "/connectors/" + CONNECTOR_NAME, null);
        awaitConnectorAbsent(CONNECTOR_NAME);
        boolean slotAfterDelete = slotExists();
        assertTrue(slotAfterDelete, "Connector deletion should leave its slot in this configuration");
        List<Event> sameNameGap = insertEvents("C7-same-name", 3, 1);
        registerConnector(CONNECTOR_NAME);
        String sameNameState = awaitTerminalConnectorState(CONNECTOR_NAME, Duration.ofSeconds(30));
        if ("RUNNING".equals(sameNameState)) {
            awaitTopicRecords(initial.size() + sameNameGap.size(), Duration.ofSeconds(30));
        }
        ReportCounts sameNameCounts = countExpectedEvents(
                readTopic(Duration.ofSeconds(2)), concatenate(initial, sameNameGap));

        request("DELETE", "/connectors/" + CONNECTOR_NAME, null);
        awaitConnectorAbsent(CONNECTOR_NAME);
        String differentName = CONNECTOR_NAME + "-different";
        List<Event> differentNameGap = insertEvents("C7-different-name", 3, 1);
        registerConnector(differentName);
        String differentNameState = awaitTerminalConnectorState(differentName, Duration.ofSeconds(30));
        String differentNameTrace = connectorTrace(differentName);
        ReportCounts differentNameCounts = countExpectedEvents(
                readTopic(Duration.ofSeconds(2)),
                concatenate(initial, sameNameGap, differentNameGap));
        evidence("C7",
                "slotAfterDelete=" + slotAfterDelete
                        + " sameNameState=" + sameNameState
                        + " sameNameLost=" + sameNameCounts.lost()
                        + " sameNameDuplicates=" + sameNameCounts.duplicates()
                        + " differentNameState=" + differentNameState
                        + " differentNameLost=" + differentNameCounts.lost()
                        + " differentNameDuplicates=" + differentNameCounts.duplicates()
                        + " differentNameFailure=" + abbreviated(differentNameTrace)
                        + " slot=" + slotStats());
    }

    @Test
    void c8OutboxRowCleanupEffects() throws Exception {
        List<Event> published = insertEvents("C8-cleanup", 4, 1);
        awaitTopicRecords(published.size(), Duration.ofSeconds(30));
        List<Long> start = captureVector("read_uncommitted");
        try (Connection connection = openDatabase();
             PreparedStatement statement = connection.prepareStatement("DELETE FROM outbox WHERE event_id = ?")) {
            for (Event event : published) {
                statement.setObject(1, UUID.fromString(event.eventId()));
                statement.addBatch();
            }
            statement.executeBatch();
        }
        List<ObservedRecord> cleanupRecords = readFromVector(start, Duration.ofSeconds(5));
        long tombstones = cleanupRecords.stream().filter(record -> record.value() == null).count();
        evidence("C8",
                "deletedRows=" + published.size()
                        + " cleanupRecords=" + cleanupRecords.size()
                        + " tombstones=" + tombstones
                        + " rc=" + captureVector("read_committed")
                        + " ru=" + captureVector("read_uncommitted"));
    }

    @Test
    void c9SnapshotAfterVectorOverlapCheck() throws Exception {
        List<Event> baseline = insertEvents("C9-aggregate", 1, 1);
        awaitTopicRecords(baseline.size(), Duration.ofSeconds(30));
        pauseConnector();

        Event laggedOne = event("C9-aggregate", 2);
        Event laggedTwo = event("C9-second", 1);
        insertEvents(List.of(laggedOne, laggedTwo));
        OffsetVectors vectors = captureVectors();

        Map<String, Integer> snapshotVersions;
        try (Connection snapshot = openDatabase()) {
            snapshot.setAutoCommit(false);
            try (Statement statement = snapshot.createStatement()) {
                statement.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                snapshotVersions = readSnapshotVersions(statement);
            }

            resumeConnector();
            List<ObservedRecord> replay = readFromVector(
                    vectors.readCommitted(), Duration.ofSeconds(30), 2);
            List<ObservedRecord> allBeforeBoundary = readBeforeVector(vectors.readCommitted());
            int beforeBoundaryViolations = countVersionsAboveSnapshot(allBeforeBoundary, snapshotVersions);
            Set<String> replayIds = eventIds(replay);
            Set<String> snapshotIds = readSnapshotEventIds(snapshot);
            Set<String> expectedLagged = Set.of(laggedOne.eventId(), laggedTwo.eventId());
            Set<String> missingReplay = new HashSet<>(expectedLagged);
            missingReplay.removeAll(replayIds);
            Set<String> missingSnapshot = new HashSet<>(expectedLagged);
            missingSnapshot.removeAll(snapshotIds);

            evidence("C9",
                    "rc=" + vectors.readCommitted()
                            + " ru=" + vectors.readUncommitted()
                            + " snapshotVersions=" + snapshotVersions
                            + " belowBoundaryViolations=" + beforeBoundaryViolations
                            + " expectedOverlap=" + expectedLagged.size()
                            + " replayedLagged=" + (expectedLagged.size() - missingReplay.size())
                            + " snapshotLagged=" + (expectedLagged.size() - missingSnapshot.size())
                            + " missingReplay=" + missingReplay
                            + " missingSnapshot=" + missingSnapshot);
            assertEquals(0, beforeBoundaryViolations,
                    "Every pre-boundary event version must be represented by the source snapshot");
            assertTrue(missingReplay.isEmpty(), "Rows committed before the snapshot must replay at/after E");
            assertTrue(missingSnapshot.isEmpty(), "Rows committed before the snapshot must occur in the snapshot");
        }
    }

    private void runCatalogMigrations() throws Exception {
        Path migrations = Path.of("..", "product-catalog", "src", "main", "resources", "db", "migration")
                .toAbsolutePath().normalize();
        if (!Files.isDirectory(migrations)) {
            throw new IllegalStateException("Catalog migration directory not found: " + migrations);
        }
        try (HikariDataSource dataSource = new HikariDataSource()) {
            dataSource.setJdbcUrl(postgres.getJdbcUrl());
            dataSource.setUsername(postgres.getUsername());
            dataSource.setPassword(postgres.getPassword());
            Flyway.configure()
                    .dataSource(dataSource)
                    .locations("filesystem:" + migrations)
                    .load()
                    .migrate();
        }
    }

    private void createPublication() throws Exception {
        try (Connection connection = openDatabase(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE PUBLICATION " + connectorDefinitionPublicationName() + " FOR TABLE public.outbox");
        }
    }

    private String connectorDefinitionPublicationName() throws Exception {
        return "debezium_product_outbox_pub";
    }

    private void createOutboxTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1)))
                    .all().get(15, TimeUnit.SECONDS);
        }
    }

    private ConnectorDefinition readConnectorDefinition() throws IOException {
        Path connectorPath = Path.of("..", "platform-infrastructure", "connectors",
                "postgres-outbox-connector.json").toAbsolutePath().normalize();
        if (!Files.isRegularFile(connectorPath)) {
            throw new IllegalStateException("Connector JSON not found: " + connectorPath);
        }
        return JSON.readValue(Files.readString(connectorPath), ConnectorDefinition.class);
    }

    private GenericContainer<?> newConnectContainer() {
        return newConnectContainer("dbz-evidence-offsets");
    }

    private GenericContainer<?> newConnectContainer(String offsetsTopic) {
        return new GenericContainer<>(CONNECT_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("connect")
                .withExposedPorts(8083)
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:19092")
                .withEnv("CONNECT_GROUP_ID", "dbz-outbox-evidence-connect")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "dbz-evidence-configs")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", offsetsTopic)
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "dbz-evidence-status")
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components")
                .withEnv("CONNECT_CONFIG_PROVIDERS", "env")
                .withEnv("CONNECT_CONFIG_PROVIDERS_ENV_CLASS",
                        "org.apache.kafka.common.config.provider.EnvVarConfigProvider")
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
                .withEnv("CDC_DATABASE_HOST", "postgres")
                .withEnv("CDC_DATABASE_USER", postgres.getUsername())
                .withEnv("CDC_DATABASE_PASSWORD", postgres.getPassword())
                .withStartupTimeout(Duration.ofMinutes(3))
                .waitingFor(Wait.forHttp("/connectors")
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
    }

    private void awaitConnectRest() throws Exception {
        waitUntil(() -> {
            try {
                return request("GET", "/connectors", null).statusCode() == 200;
            } catch (Exception ignored) {
                return false;
            }
        }, Duration.ofMinutes(2), "Kafka Connect REST");
        HttpResponse<String> plugins = request("GET", "/connector-plugins", null);
        assertTrue(plugins.body().contains("io.debezium.connector.postgresql.PostgresConnector"),
                "Debezium PostgreSQL plugin must be installed");
    }

    private void registerConnector(String name) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("config", connectorDefinition.config());
        HttpResponse<String> response = request("POST", "/connectors", JSON.writeValueAsString(body));
        assertTrue(response.statusCode() == 201 || response.statusCode() == 200,
                "Connector registration failed (" + response.statusCode() + "): " + response.body());
    }

    private void pauseConnector() throws Exception {
        HttpResponse<String> response = request("PUT", "/connectors/" + CONNECTOR_NAME + "/pause", "");
        assertTrue(response.statusCode() == 202 || response.statusCode() == 204,
                "Connector pause failed: " + response.statusCode() + " " + response.body());
        awaitConnectorState(CONNECTOR_NAME, "PAUSED", Duration.ofSeconds(20));
    }

    private void resumeConnector() throws Exception {
        HttpResponse<String> response = request("PUT", "/connectors/" + CONNECTOR_NAME + "/resume", "");
        assertTrue(response.statusCode() == 202 || response.statusCode() == 204,
                "Connector resume failed: " + response.statusCode() + " " + response.body());
        awaitConnectorState(CONNECTOR_NAME, "RUNNING", CONNECTOR_TIMEOUT);
    }

    private void killAndRestartConnect() throws Exception {
        String containerId = connect.getContainerId();
        DockerClientFactory.instance().client().killContainerCmd(containerId).exec();
        connect.stop();
        connect = newConnectContainer();
        connect.start();
        awaitConnectRest();
    }

    private void awaitConnectorState(String name, String expected, Duration timeout) throws Exception {
        waitUntil(() -> {
            try {
                return expected.equals(connectorState(name));
            } catch (Exception ignored) {
                return false;
            }
        }, timeout, "connector " + name + " state " + expected);
    }

    private String awaitTerminalConnectorState(String name, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        String lastState = "UNKNOWN";
        while (Instant.now().isBefore(deadline)) {
            try {
                lastState = connectorState(name);
                if (Set.of("RUNNING", "FAILED", "STOPPED", "PAUSED").contains(lastState)) {
                    if ("RUNNING".equals(lastState)) {
                        Thread.sleep(1_000);
                        return connectorState(name);
                    }
                    return lastState;
                }
            } catch (Exception ignored) {
                lastState = "NOT_FOUND";
            }
            Thread.sleep(250);
        }
        return lastState;
    }

    private String connectorState(String name) throws Exception {
        HttpResponse<String> response = request("GET", "/connectors/" + name + "/status", null);
        if (response.statusCode() != 200) {
            return "NOT_FOUND";
        }
        Map<String, Object> status = JSON.readValue(response.body(), JSON_MAP);
        Object connector = status.get("connector");
        if (connector instanceof Map<?, ?> state) {
            return String.valueOf(state.get("state"));
        }
        return "UNKNOWN";
    }

    private String connectorTrace(String name) throws Exception {
        HttpResponse<String> response = request("GET", "/connectors/" + name + "/status", null);
        return response.statusCode() == 200 ? response.body() : "status-http-" + response.statusCode();
    }

    private void awaitConnectorAbsent(String name) throws Exception {
        waitUntil(() -> {
            try {
                return request("GET", "/connectors/" + name, null).statusCode() == 404;
            } catch (Exception ignored) {
                return false;
            }
        }, Duration.ofSeconds(20), "connector deletion");
    }

    private HttpResponse<String> request(String method, String path, String body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(
                        URI.create("http://" + connect.getHost() + ":" + connect.getMappedPort(8083) + path))
                .timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json");
        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private Connection openDatabase() throws Exception {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private List<Event> insertEvents(String prefix, int count, int version) throws Exception {
        List<Event> events = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            events.add(event(prefix + "-" + i, version));
        }
        insertEvents(events);
        return events;
    }

    private void insertEvents(List<Event> events) throws Exception {
        try (Connection connection = openDatabase()) {
            for (Event event : events) {
                insertEvent(connection, event);
            }
        }
    }

    private void insertEvent(Connection connection, Event event) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, schema_version, processed)
                VALUES (?, 'Product', ?, 'UPDATED', ?, 1, false)
                """)) {
            statement.setObject(1, UUID.fromString(event.eventId()));
            statement.setString(2, event.aggregateId());
            statement.setString(3, JSON.writeValueAsString(event.payload()));
            statement.executeUpdate();
        }
    }

    private Event event(String aggregateId, int version) {
        return new Event(UUID.randomUUID().toString(), aggregateId, version);
    }

    private OffsetVectors captureVectors() throws Exception {
        return new OffsetVectors(captureVector("read_committed"), captureVector("read_uncommitted"));
    }

    private List<Long> captureVector(String isolation) throws Exception {
        try (KafkaConsumer<String, byte[]> consumer = newKafkaConsumer(isolation)) {
            List<TopicPartition> partitions = topicPartitions(consumer);
            consumer.assign(partitions);
            Map<TopicPartition, Long> offsets = consumer.endOffsets(partitions, Duration.ofSeconds(5));
            return partitions.stream().map(partition -> offsets.get(partition)).toList();
        }
    }

    private List<ObservedRecord> awaitTopicRecords(int expectedCount, Duration timeout) throws Exception {
        return readTopicUntil(expectedCount, timeout, Duration.ofSeconds(1));
    }

    private List<ObservedRecord> readTopic(Duration duration) throws Exception {
        return readFromVector(List.of(0L, 0L, 0L), duration);
    }

    private List<ObservedRecord> readFromVector(List<Long> startOffsets, Duration duration) throws Exception {
        return readFromVector(startOffsets, duration, Integer.MAX_VALUE);
    }

    private List<ObservedRecord> readFromVector(
            List<Long> startOffsets, Duration timeout, int minimumCount) throws Exception {
        try (KafkaConsumer<String, byte[]> consumer = newKafkaConsumer("read_committed")) {
            List<TopicPartition> partitions = topicPartitions(consumer);
            consumer.assign(partitions);
            for (TopicPartition partition : partitions) {
                long offset = partition.partition() < startOffsets.size()
                        ? startOffsets.get(partition.partition())
                        : 0L;
                consumer.seek(partition, offset);
            }
            List<ObservedRecord> records = new ArrayList<>();
            Instant deadline = Instant.now().plus(timeout);
            Instant quietDeadline = null;
            do {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    records.add(observed(record));
                }
                if (records.size() >= minimumCount && quietDeadline == null) {
                    quietDeadline = Instant.now().plusSeconds(1);
                }
                if (quietDeadline != null && Instant.now().isAfter(quietDeadline)) {
                    break;
                }
            } while (Instant.now().isBefore(deadline));
            return records;
        }
    }

    private List<ObservedRecord> readTopicUntil(int expectedCount, Duration timeout, Duration settle)
            throws Exception {
        try (KafkaConsumer<String, byte[]> consumer = newKafkaConsumer("read_committed")) {
            List<TopicPartition> partitions = topicPartitions(consumer);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            List<ObservedRecord> records = new ArrayList<>();
            Instant deadline = Instant.now().plus(timeout);
            Instant quietDeadline = null;
            do {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    records.add(observed(record));
                }
                if (records.size() >= expectedCount && quietDeadline == null) {
                    quietDeadline = Instant.now().plus(settle);
                }
                if (quietDeadline != null && Instant.now().isAfter(quietDeadline)) {
                    break;
                }
            } while (Instant.now().isBefore(deadline));
            return records;
        }
    }

    private List<ObservedRecord> readBeforeVector(List<Long> vector) throws Exception {
        List<ObservedRecord> all = readFromVector(List.of(0L, 0L, 0L), Duration.ofSeconds(3));
        return all.stream().filter(record -> record.offset() < vector.get(record.partition())).toList();
    }

    private KafkaConsumer<String, byte[]> newKafkaConsumer(String isolation) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "dbz-evidence-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    private List<TopicPartition> topicPartitions(KafkaConsumer<String, byte[]> consumer) {
        List<PartitionInfo> info = consumer.partitionsFor(TOPIC, Duration.ofSeconds(5));
        return info.stream()
                .map(partition -> new TopicPartition(TOPIC, partition.partition()))
                .sorted((left, right) -> Integer.compare(left.partition(), right.partition()))
                .toList();
    }

    private ObservedRecord observed(ConsumerRecord<String, byte[]> record) {
        return new ObservedRecord(record.partition(), record.offset(), record.key(),
                record.value() == null ? null : new String(record.value(), java.nio.charset.StandardCharsets.UTF_8));
    }

    private boolean expectedKey(ObservedRecord record) {
        if (record.key() == null || record.value() == null) {
            return false;
        }
        try {
            Map<String, Object> payload = JSON.readValue(record.value(), JSON_MAP);
            return record.key().equals(payload.get("aggregateId"));
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean expectedPayload(ObservedRecord record) {
        if (record.value() == null) {
            return false;
        }
        try {
            Map<String, Object> payload = JSON.readValue(record.value(), JSON_MAP);
            return payload.containsKey("eventId") && payload.containsKey("version");
        } catch (Exception ignored) {
            return false;
        }
    }

    private long countOffsetGaps(List<ObservedRecord> records, List<Long> endOffsets) {
        long gaps = 0;
        for (int partition = 0; partition < endOffsets.size(); partition++) {
            int partitionIndex = partition;
            long delivered = records.stream()
                    .filter(record -> record.partition() == partitionIndex && record.value() != null)
                    .count();
            gaps += Math.max(0, endOffsets.get(partition) - delivered);
        }
        return gaps;
    }

    private ReportCounts countExpectedEvents(List<ObservedRecord> records, List<Event> expected) throws Exception {
        Set<String> expectedIds = new HashSet<>();
        expected.forEach(event -> expectedIds.add(event.eventId()));
        Map<String, Integer> deliveredCounts = new HashMap<>();
        for (ObservedRecord record : records) {
            String id = eventId(record);
            if (id != null && expectedIds.contains(id)) {
                deliveredCounts.merge(id, 1, Integer::sum);
            }
        }
        int delivered = deliveredCounts.size();
        int duplicates = deliveredCounts.values().stream().mapToInt(count -> Math.max(0, count - 1)).sum();
        return new ReportCounts(expected.size(), delivered, expected.size() - delivered, duplicates);
    }

    private String eventId(ObservedRecord record) throws Exception {
        if (record.value() == null) {
            return null;
        }
        return String.valueOf(JSON.readValue(record.value(), JSON_MAP).get("eventId"));
    }

    private int countOrderingViolations(List<ObservedRecord> records) throws Exception {
        Map<String, Integer> latestVersion = new HashMap<>();
        Map<String, Long> latestOffset = new HashMap<>();
        int violations = 0;
        for (ObservedRecord record : records.stream()
                .filter(item -> item.value() != null)
                .sorted((left, right) -> Long.compare(left.offset(), right.offset()))
                .toList()) {
            Map<String, Object> payload = JSON.readValue(record.value(), JSON_MAP);
            String aggregate = record.key();
            int version = ((Number) payload.get("version")).intValue();
            Integer previous = latestVersion.get(aggregate);
            Long previousOffset = latestOffset.get(aggregate);
            if (previous != null && record.offset() > previousOffset && version < previous) {
                violations++;
            }
            latestVersion.put(aggregate, version);
            latestOffset.put(aggregate, record.offset());
        }
        return violations;
    }

    private WalStats slotStats() throws Exception {
        try (Connection connection = openDatabase();
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT restart_lsn::text,
                            confirmed_flush_lsn::text,
                            COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, 0)
                     FROM pg_replication_slots
                     WHERE slot_name = ?
                     """)) {
            statement.setString(1, SLOT_NAME);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return new WalStats("missing", "missing", -1);
                }
                return new WalStats(result.getString(1), result.getString(2), result.getLong(3));
            }
        }
    }

    private boolean slotExists() throws Exception {
        try (Connection connection = openDatabase();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = ?)")) {
            statement.setString(1, SLOT_NAME);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getBoolean(1);
            }
        }
    }

    private Map<String, Integer> readSnapshotVersions(Statement statement) throws Exception {
        Map<String, Integer> versions = new HashMap<>();
        try (ResultSet result = statement.executeQuery("SELECT aggregate_id, payload FROM outbox")) {
            while (result.next()) {
                Map<String, Object> payload = JSON.readValue(result.getString("payload"), JSON_MAP);
                versions.merge(result.getString("aggregate_id"),
                        ((Number) payload.get("version")).intValue(), Math::max);
            }
        }
        return versions;
    }

    private Set<String> readSnapshotEventIds(Connection connection) throws Exception {
        Set<String> eventIds = new HashSet<>();
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT event_id FROM outbox")) {
            while (result.next()) {
                eventIds.add(result.getObject(1).toString());
            }
        }
        return eventIds;
    }

    private int countVersionsAboveSnapshot(
            List<ObservedRecord> records, Map<String, Integer> snapshotVersions) throws Exception {
        int violations = 0;
        for (ObservedRecord record : records) {
            if (record.value() == null) {
                continue;
            }
            Map<String, Object> payload = JSON.readValue(record.value(), JSON_MAP);
            int version = ((Number) payload.get("version")).intValue();
            Integer snapshotVersion = snapshotVersions.get(record.key());
            if (snapshotVersion == null || version > snapshotVersion) {
                violations++;
            }
        }
        return violations;
    }

    private Set<String> eventIds(List<ObservedRecord> records) throws Exception {
        Set<String> ids = new HashSet<>();
        for (ObservedRecord record : records) {
            String id = eventId(record);
            if (id != null) {
                ids.add(id);
            }
        }
        return ids;
    }

    private void waitUntil(CheckedBoolean condition, Duration timeout, String description) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.get()) {
                return;
            }
            Thread.sleep(250);
        }
        throw new AssertionError("Timed out waiting for " + description);
    }

    @SafeVarargs
    private final List<Event> concatenate(List<Event>... groups) {
        List<Event> result = new ArrayList<>();
        for (List<Event> group : groups) {
            result.addAll(group);
        }
        return result;
    }

    private String abbreviated(String text) {
        if (text == null) {
            return "none";
        }
        return text.replaceAll("\\s+", " ").substring(0, Math.min(300, text.replaceAll("\\s+", " ").length()));
    }

    private void evidence(String scenario, String values) {
        System.out.println("DBZ_EVIDENCE scenario=" + scenario + " " + values);
    }

    private record ConnectorDefinition(String name, Map<String, String> config) {
    }

    private record Event(String eventId, String aggregateId, int version) {
        Map<String, Object> payload() {
            return Map.of(
                    "eventId", eventId,
                    "aggregateId", aggregateId,
                    "version", version,
                    "eventType", "UPDATED",
                    "schemaVersion", 1);
        }
    }

    private record ObservedRecord(int partition, long offset, String key, String value) {
    }

    private record OffsetVectors(List<Long> readCommitted, List<Long> readUncommitted) {
    }

    private record ReportCounts(int expected, int delivered, int lost, int duplicates) {
    }

    private record WalStats(String restartLsn, String confirmedFlushLsn, long retainedWalBytes) {
    }

    @FunctionalInterface
    private interface CheckedBoolean {
        boolean get() throws Exception;
    }
}
