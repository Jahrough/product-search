# Stage 1: Build the application using Gradle & Temurin JDK
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /app

# Copy gradle wrapper and build configuration files first to cache dependencies
COPY gradlew build.gradle.kts settings.gradle.kts ./
COPY gradle gradle

# Download dependencies offline to leverage Docker layer caching
RUN ./gradlew dependencies --no-daemon

# Copy source code after dependencies are cached
COPY src src

# Build the executable fat JAR (skipping tests for fast container builds)
RUN ./gradlew bootJar -x test --no-daemon

# Stage 2: Run the application using lightweight Temurin JRE Alpine base
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Create a non-root system user for security compliance in Kubernetes / AWS EKS
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

# Copy the built jar artifact from the build stage
COPY --from=build /app/build/libs/*.jar app.jar

# Expose the application port
EXPOSE 8080

# Configure JVM flags optimized for containerized cloud environments
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]