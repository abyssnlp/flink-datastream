FROM maven:3-eclipse-temurin-17 as build

COPY pom.xml .
RUN mvn dependency:resolve
RUN mvn dependency:go-offline

COPY . .
RUN mvn clean verify -o

FROM flink:1.20-java17
RUN cd /opt/flink/lib
COPY --from=build --chown=flink:flink target/flink-datastream-enrichment-1.0.jar /opt/flink-jobs/flink-datastream-enrichment-1.0.jar