plugins {
    java
    jacoco
    id("org.springframework.boot") version "4.0.9"
    id("io.spring.dependency-management") version "1.1.6"
}

group = "dev.jahlopez"
version = "1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

// Updated Dependency versions
val springBootVersion = "4.0.9"
val lombokVersion = "1.18.36"
val mapstructVersion = "1.6.3"
val openSearchVersion = "2.17.0"
val openSearchTestcontainersVersion = "2.1.4"
val springdocVersion = "3.0.3"
val logstashEncoderVersion = "8.0"
val testcontainersVersion = "1.21.4"
val testcontainersRedisVersion = "2.2.4"
val jakartaJsonVersion = "2.1.3"
val parssonVersion = "1.1.7"
val resilience4jVersion = "2.2.0"

// Override managed dependencies for vulnerability fixes & Testcontainers compatibility
extra["testcontainers.version"] = testcontainersVersion

dependencies {
    // REST API and validation
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-validation")

    // Service Resilience
    implementation("io.github.resilience4j:resilience4j-spring-boot3:$resilience4jVersion")
    implementation("org.springframework.boot:spring-boot-starter-aspectj")

    // Database & Persistence
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    runtimeOnly("org.postgresql:postgresql")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")

    // Kafka event consumption
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.networknt:json-schema-validator:3.0.5")

    // OpenSearch client and REST transport
    implementation("org.opensearch.client:opensearch-java:$openSearchVersion")
    implementation("org.opensearch.client:opensearch-rest-client:$openSearchVersion")

    // JSON-P provider
    implementation("jakarta.json:jakarta.json-api:$jakartaJsonVersion")
    runtimeOnly("org.eclipse.parsson:parsson:$parssonVersion")

    // API documentation
    implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:$springdocVersion")

    // Redis caching
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("org.springframework.boot:spring-boot-starter-cache")

    // Observability
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation("io.micrometer:micrometer-tracing-bridge-otel")
    implementation("io.opentelemetry:opentelemetry-exporter-zipkin")
    implementation("net.logstash.logback:logstash-logback-encoder:$logstashEncoderVersion")

    // Object mapping
    implementation("org.mapstruct:mapstruct:$mapstructVersion")

    // Lombok
    compileOnly("org.projectlombok:lombok:$lombokVersion")
    annotationProcessor("org.projectlombok:lombok:$lombokVersion")

    // MapStruct annotation processing
    annotationProcessor("org.mapstruct:mapstruct-processor:$mapstructVersion")
    annotationProcessor("org.projectlombok:lombok-mapstruct-binding:0.2.0")

    // Spring Boot configuration metadata
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor:$springBootVersion")

    // Testing
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.springframework.boot:spring-boot-starter-data-jpa-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")

    // Test annotation processing
    testCompileOnly("org.projectlombok:lombok:$lombokVersion")
    testAnnotationProcessor("org.projectlombok:lombok:$lombokVersion")
    testAnnotationProcessor("org.mapstruct:mapstruct-processor:$mapstructVersion")
    testAnnotationProcessor("org.projectlombok:lombok-mapstruct-binding:0.2.0")

    // JUnit Platform
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // Testcontainers
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:kafka")
    testImplementation("org.testcontainers:postgresql")

    // OpenSearch Testcontainers
    testImplementation("org.opensearch:opensearch-testcontainers:$openSearchTestcontainersVersion")

    // Redis Testcontainers
    testImplementation("com.redis:testcontainers-redis:$testcontainersRedisVersion")
}

jacoco {
    toolVersion = "0.8.12"
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:deprecation")
}

tasks.named<JavaCompile>("compileJava") {
    options.compilerArgs.addAll(
        listOf(
            "-Amapstruct.defaultComponentModel=spring",
            "-Amapstruct.unmappedTargetPolicy=WARN"
        )
    )
}

tasks.test {
    useJUnitPlatform()
    finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)

    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}
