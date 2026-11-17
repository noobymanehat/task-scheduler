# Build stage: compile and package with the Maven wrapper.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /build
COPY .mvn .mvn
COPY mvnw pom.xml ./
# Dependencies in their own layer, so source changes do not re-download them.
RUN ./mvnw -B -q dependency:go-offline
COPY src src
# Tests need Docker (Testcontainers) and run in CI / locally via ./mvnw verify, not here.
RUN ./mvnw -B -q package -DskipTests

# Runtime stage: JRE only, non-root.
FROM eclipse-temurin:21-jre
RUN useradd --system --no-create-home scheduler
USER scheduler
WORKDIR /app
COPY --from=build /build/target/task-scheduler-*.jar app.jar
EXPOSE 8080
# Exec form, so the JVM is PID 1 and receives SIGTERM for graceful shutdown.
ENTRYPOINT ["java", "-jar", "app.jar"]
