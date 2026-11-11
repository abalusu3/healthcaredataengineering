FROM eclipse-temurin:17-jre

# Pinned Synthea release (synthetic patient generator, MITRE).
ARG SYNTHEA_VERSION=v4.0.0
ADD https://github.com/synthetichealth/synthea/releases/download/${SYNTHEA_VERSION}/synthea-with-dependencies.jar /opt/synthea/synthea.jar

COPY docker/synthea/generate.sh /opt/synthea/generate.sh
RUN chmod +x /opt/synthea/generate.sh

WORKDIR /opt/synthea
ENTRYPOINT ["/opt/synthea/generate.sh"]
