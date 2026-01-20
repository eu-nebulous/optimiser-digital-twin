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

# See the output of `java -jar
# optimiser-digital-twin/dist/optimiser-digital-twin-all.jar serve -h` for a
# list of command-line options and environment variables for setting options.
#
# The container can be started with explicit parameters, environment variables
# or a mix of both.  Parameters override variables.
#
#     docker run -e APPLICATION_ID="my_app_id" nebulous/digital-twin serve -h="https://amqp.example.com/" --pw=s3kr1t
#
ENTRYPOINT ["java","-jar","/usr/local/lib/optimiser-digital-twin-all.jar", "-vv", "serve"]
