#
# Build stage
#
FROM docker.io/library/gradle:9-jdk21-alpine AS build
COPY . /home/optimiser-digital-twin
WORKDIR /home/optimiser-digital-twin
RUN gradle --no-daemon -Dorg.gradle.logging.level=info clean build

#
# Package stage
#
FROM docker.io/library/eclipse-temurin:21-jre
COPY --from=build /home/optimiser-digital-twin/optimiser-digital-twin/dist/optimiser-digital-twin-all.jar /usr/local/lib/optimiser-digital-twin-all.jar
ENTRYPOINT ["java","-jar","/usr/local/lib/optimiser-digital-twin-all.jar", "-vv", "serve"]
