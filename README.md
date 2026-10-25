# Product Search

Product Search is a Java/Spring Boot microservice that builds and serves a
search projection of a product catalog. It consumes lifecycle events from
Kafka, indexes product documents in OpenSearch, stores processed-event metadata
in PostgreSQL, and exposes a paginated REST API. Redis provides response
caching.

> **Production security notice:** This application does not enforce
> authentication or authorization. OpenAPI security annotations are
> documentation only. Do not expose the API or management endpoints directly
> to untrusted networks; use an authenticated gateway or implement and verify
> application security before production use.

## Contents

- [Architecture](#architecture)
- [Technology stack](#technology-stack)
- [Project structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Run locally](#run-locally)
- [Docker](#docker)
- [API](#api)
- [API documentation](#api-documentation)
- [Testing](#testing)
- [Observability and health](#observability-and-health)
- [CI and container publishing](#ci-and-container-publishing)
- [Design considerations and next steps](#design-considerations-and-next-steps)
- [License](#license)

## Architecture

```text
Product producer
      |
      v
Kafka: product-events ---> Product Search consumer ---> OpenSearch: products
      |                             |
      |                             +-- PostgreSQL: processed_events
      +-- failed events after retries ---> product-events.DLT

API clients ---> REST API ---> OpenSearch
                       |
                       +------> Redis response cache
```

The upstream product service remains the system of record and publishes
authoritative events. OpenSearch is the read/search projection, Redis is a
rebuildable cache, and PostgreSQL stores the consumer inbox used for event
deduplication and version checks. Flyway applies the PostgreSQL migration at
application startup.

### Product event contract

The default source topic is `product-events`. Messages are JSON representations
of `ProductEvent`:

```json
{
  "eventId": "7df16f23-9bd6-4d8c-9c7c-8128f2ae8020",
  "version": 1,
  "productId": "prod-12345",
  "sku": "SKU-98765",
  "name": "Ergonomic Mechanical Keyboard",
  "description": "Wireless mechanical keyboard",
  "price": 149.99,
  "stockQuantity": 42,
  "eventType": "CREATED",
  "timestamp": "2026-10-25T12:00:00"
}
```

`eventType` is `CREATED`, `UPDATED`, or `DELETED`. For indexing, `eventId`,
`productId`, and `eventType` must be present. Producers should supply a unique
UUID event ID and monotonically increasing product version. Create/update
events need the product fields; deletion is performed by `productId`. The
timestamp is descriptive and is not used for ordering.

Listener failures are retried three times with a one-second backoff after the
initial attempt. After retries, records are published to
`<source-topic>.DLT` at the original partition number. The DLT listener logs
failures but does not repair or replay them. The general processing listener,
indexing listener, and DLT listener use separate consumer groups, so each group
receives the relevant topic independently.

## Technology stack

- Java 21
- Spring Boot 4 and Spring Framework
- Gradle
- OpenSearch Java client
- Apache Kafka and Spring Kafka
- PostgreSQL, Spring Data JPA, and Flyway
- Redis and Spring Cache
- Spring Boot Actuator, Micrometer, Prometheus, and OpenTelemetry tracing
- Springdoc OpenAPI and Swagger UI
- JUnit 5 and Testcontainers

## Project structure

```text
.
├── src
│   ├── main
│   │   ├── java/dev/jahlopez/productsearch
│   │   │   ├── config/       # Kafka, OpenSearch, Redis, and OpenAPI setup
│   │   │   ├── controller/   # REST endpoints
│   │   │   ├── document/     # OpenSearch product document
│   │   │   ├── dto/          # API and event data transfer objects
│   │   │   ├── event/        # Product lifecycle event model
│   │   │   ├── exception/    # API error handling
│   │   │   ├── listener/     # Kafka event consumers
│   │   │   ├── mapper/       # Document-to-response mapping
│   │   │   ├── repository/   # OpenSearch and PostgreSQL persistence
│   │   │   └── service/      # Search and indexing operations
│   │   └── resources
│   │       ├── db/migration/ # Flyway migrations
│   │       └── application.yml
│   └── test
│       ├── java/             # Unit and integration tests
│       └── resources/
├── .github/workflows/        # CI, container scanning, and publishing
├── Dockerfile
├── build.gradle.kts
└── gradlew
```

## Prerequisites

- JDK 21 to run or build from source.
- Docker for Testcontainers integration tests.
- Reachable PostgreSQL, Kafka, OpenSearch, and Redis services.
- An OpenSearch index named `products`.
- Kafka source topic `product-events` and DLT `product-events.DLT`.

This repository does not provision or start the backing services. For
production, use managed or highly available services and configure network
access, TLS, authentication, backups, and monitoring for the target platform.

Create the OpenSearch index before running the service. This mapping supports
the currently indexed product fields and exact SKU lookup:

```bash
curl --fail-with-body --request PUT 'http://localhost:9200/products' \
  --header 'Content-Type: application/json' \
  --data '{
    "mappings": {
      "properties": {
        "id":            { "type": "keyword" },
        "sku":           { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
        "name":          { "type": "text" },
        "description":   { "type": "text" },
        "price":         { "type": "double" },
        "stockQuantity": { "type": "integer" },
        "version":       { "type": "long" }
      }
    }
  }'
```

## Configuration

Spring Boot configuration is supplied through environment variables. PostgreSQL
connection settings have no application-defined defaults and must be provided.

| Variable | Default | Description |
| --- | --- | --- |
| `SERVER_PORT` | `8081` | HTTP listening port. |
| `SPRING_DATASOURCE_URL` | None | PostgreSQL JDBC URL, e.g. `jdbc:postgresql://localhost:5432/product_search`. |
| `SPRING_DATASOURCE_USERNAME` | None | PostgreSQL account. |
| `SPRING_DATASOURCE_PASSWORD` | None | PostgreSQL password. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker list. |
| `SPRING_KAFKA_CONSUMER_GROUP_ID` | `product-search-group` | General event listener group and consumer-factory fallback. |
| `SPRING_KAFKA_CONSUMER_AUTO_OFFSET_RESET` | `earliest` | Offset policy when a group has no committed offset. |
| `APP_KAFKA_TOPIC_PRODUCT_EVENTS` | `product-events` | Product event topic; DLT uses this name plus `.DLT`. |
| `REDIS_HOST` | `localhost` | Redis hostname. |
| `REDIS_PORT` | `6379` | Redis port. |
| `OPENSEARCH_URIS` | `http://localhost:9200` | OpenSearch endpoint. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318/v1/traces` | OTLP/HTTP trace export endpoint. |

The indexing group (`product-search-search-group`) and DLT group
(`product-search-dlq-group`) are fixed in code. Instances in one group share
partitions; separate groups independently consume all events.

The application does not configure Redis or OpenSearch credentials. Do not
assume that environment variables alone enable authentication. Use private
networking and platform controls, or implement and verify client authentication
support before using secured clusters.

## Run locally

Start PostgreSQL, Kafka, OpenSearch, and Redis separately, create the
`products` index as described above, then set the database connection and run:

```bash
export SPRING_DATASOURCE_URL='jdbc:postgresql://localhost:5432/product_search'
export SPRING_DATASOURCE_USERNAME='product_search'
export SPRING_DATASOURCE_PASSWORD='change-me'

./gradlew bootRun
```

Override the other service addresses with the variables in
[Configuration](#configuration). The REST API listens on
`http://localhost:8081` by default. Flyway runs the schema migration on startup.

## Docker

Build the image:

```bash
docker build --tag product-search:local .
```

Run the container after exporting the required connection values in the host
environment:

```bash
docker run --rm --publish 8081:8081 \
  --env SPRING_DATASOURCE_URL \
  --env SPRING_DATASOURCE_USERNAME \
  --env SPRING_DATASOURCE_PASSWORD \
  --env KAFKA_BOOTSTRAP_SERVERS \
  --env REDIS_HOST \
  --env REDIS_PORT \
  --env OPENSEARCH_URIS \
  product-search:local
```

The multi-stage image uses a Java 21 JRE runtime, runs as a non-root user, and
exposes port `8081`. For production, deploy an immutable image by digest from a
trusted registry, inject secrets through a secret manager, and configure
resource limits, replicas, graceful shutdown, and platform health probes.

## API

All product routes are under `/api/v1/products/search`.

### Search products

`POST /api/v1/products/search?page=0&size=20`

```json
{
  "keyword": "mechanical keyboard",
  "category": "Electronics"
}
```

```bash
curl --fail-with-body --request POST \
  'http://localhost:8081/api/v1/products/search?page=0&size=20' \
  --header 'Content-Type: application/json' \
  --data '{"keyword":"mechanical keyboard","category":"Electronics"}'
```

Keyword and category are optional. Pagination is zero-based and uses Spring
Data `page`, `size`, and `sort` query parameters. Set `size` explicitly; there
is no service-specific maximum page size. The response is a Spring `Page` JSON
object containing `content`, `totalElements`, `totalPages`, `number`, and page
metadata. Each result contains `id`, `sku`, `name`, `description`, `price`, and
`stockQuantity`.

### Get a product by ID

`GET /api/v1/products/search/{id}`

```bash
curl --fail-with-body \
  'http://localhost:8081/api/v1/products/search/prod-12345'
```

### Get a product by SKU

`GET /api/v1/products/search/sku/{sku}`

```bash
curl --fail-with-body \
  'http://localhost:8081/api/v1/products/search/sku/SKU-98765'
```

The standard exception handler returns `timestamp`, `status`, `error`,
`message`, and `path`. Missing products return `404`, request-body validation
errors return `400`, and unexpected exceptions return `500`.

Search circuit-breaker fallback returns an empty page when OpenSearch calls
fail, so an empty `200` can mean either no matches or degraded search. Product
ID and SKU lookup failures can return `500` during an OpenSearch outage.

## API documentation

- Swagger UI: `http://localhost:8081/swagger-ui.html`
- OpenAPI JSON: `http://localhost:8081/v3/api-docs`

The OpenAPI annotations declare endpoint security, but no authentication or
authorization is enforced by the application. Do not interpret the API
documentation as an access-control mechanism.

## Testing

Run the Gradle build and tests:

```bash
./gradlew build
./gradlew test
```

Integration tests use Testcontainers and require Docker. JaCoCo reports are
generated under `build/reports/jacoco/test/`.

## Observability and health

Actuator web exposure is configured for `health`, `info`, `prometheus`, and
`metrics`:

- `GET /actuator/health`
- `GET /actuator/health/liveness`
- `GET /actuator/health/readiness`
- `GET /actuator/metrics`
- `GET /actuator/prometheus`

Health details are configured to be shown always. Trace sampling is currently
set to probability `1.0`; tune it for production traffic and cost, and set
`OTEL_EXPORTER_OTLP_ENDPOINT` to the collector endpoint. Restrict Actuator and
Prometheus access to trusted monitoring/orchestration clients.

## CI and container publishing

`.github/workflows/ci-cd.yml` runs the build and tests on pushes and pull
requests targeting `main`. It builds a Docker image for Trivy scanning and
fails on fixable critical/high findings (unfixed vulnerabilities are ignored).
On a successful push to `main`, it publishes the
image to GitHub Container Registry (`ghcr.io`) with the commit SHA and `latest`
tags, then signs the commit-tagged image with keyless Cosign.

## Design considerations and next steps

### Known functional limitations

- `minPrice`, `maxPrice`, and `inStockOnly` are present in the search request
  DTO but are not applied by the OpenSearch query.
- The query accepts a category filter using `category.keyword`, but category
  is not present in the current event or document model. Category filtering
  therefore is not functional.
- Search fallback returns an empty page for OpenSearch failures, which makes
  outages indistinguishable from genuine zero-result searches to API clients.
- Product deletion evicts the ID cache entry but does not explicitly evict a
  SKU-keyed lookup. A deleted product can remain visible through the SKU cache
  until that entry expires.
- OpenSearch and PostgreSQL writes are not one atomic transaction. Event
  handling is at-least-once and requires stable event IDs, version checks, and
  operational reconciliation.

### Production readiness

Before public or partner-facing production deployment:

- Enforce authentication and authorization at a trusted gateway or implement
  and test them in the service.
- Protect the API and Actuator with TLS and network policies. Restrict
  detailed health and metrics endpoints.
- Protect Kafka, PostgreSQL, Redis, and OpenSearch with private connectivity,
  TLS, least-privilege identities, and credential rotation.
- Store secrets in a secret manager; do not commit them or pass them as image
  build arguments.
- Configure production tracing sampling, alerting, retention, resource limits,
  and autoscaling.
- Back up PostgreSQL and OpenSearch, monitor Kafka lag and DLT growth, and
  establish controlled event inspection and replay procedures.

These are deployment recommendations, not capabilities supplied by the
application. Validate them in the target environment.

## License

No `LICENSE` file is present in this repository. Until a license is added,
reuse, redistribution, and contribution terms are not specified.
