# Stage 1: Build the application
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /app

# Copy Gradle wrapper and configuration files first for optimal caching
COPY gradlew build.gradle.kts settings.gradle.kts ./
COPY gradle/ ./gradle/

# Ensure the Gradle wrapper script is executable
RUN chmod +x gradlew

# Download and cache dependencies (will only re-run if build files change)
RUN ./gradlew dependencies --no-daemon

# Copy source code
COPY . .

# Build the executable boot jar
RUN ./gradlew bootJar --no-daemon

# Stage 2: Production runtime image
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Create a non-root system user and group for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy the built jar from the builder stage with correct non-root ownership
COPY --chown=appuser:appgroup --from=builder /app/build/libs/*.jar app.jar

USER appuser:appgroup

EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]