FROM flink:1.17.1-scala_2.12

USER root

# Install required tools
RUN apt-get update && \
    apt-get install -y curl unzip protobuf-compiler openjdk-11-jdk && \
    curl -L -o sbt.tgz https://github.com/sbt/sbt/releases/download/v1.8.2/sbt-1.8.2.tgz && \
    tar -xvzf sbt.tgz -C /opt && \
    ln -s /opt/sbt/bin/sbt /usr/bin/sbt

# Set working directory
WORKDIR /app

# Copy all source files
COPY build.sbt .
COPY gtfs-realtime.proto .
COPY project ./project
COPY src ./src

# Generate Java classes from proto
RUN protoc --java_out=src/main/java gtfs-realtime.proto

# Clean sbt cache just in case
RUN rm -rf /root/.ivy2 /root/.sbt

# Build the JAR
RUN sbt clean compile package

# Copy resulting JAR to Flink job directory
RUN mkdir -p /opt/flink-job && cp target/scala-2.12/*.jar /opt/flink-job/job.jar

# Keep container running (for testing)
CMD ["tail", "-f", "/dev/null"]
