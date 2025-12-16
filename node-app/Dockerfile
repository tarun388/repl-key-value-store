FROM maven:3.8-openjdk-17 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:25-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/*-jar-with-dependencies.jar app.jar
EXPOSE 7000
ENTRYPOINT [ "java", "-jar", "app.jar" ]