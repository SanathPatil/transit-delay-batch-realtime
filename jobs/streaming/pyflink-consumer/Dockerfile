FROM apache/flink:1.17.1-scala_2.12

USER root

RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-dev curl unzip git protobuf-compiler \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN pip3 install --upgrade pip && pip3 install -r requirements.txt
RUN pip3 install apache-flink

ENV PYTHONPATH=/app

RUN mkdir -p /opt/flink/plugins/custom
COPY flink-connector-kafka-1.17.1.jar /opt/flink/plugins/custom/