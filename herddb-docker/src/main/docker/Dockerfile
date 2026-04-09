FROM eclipse-temurin:22-jdk

# Install diagnostic tools and Google Cloud SDK (for gsutil)
RUN apt-get update && \
    apt-get install -y --no-install-recommends less iputils-ping net-tools curl gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    apt-get update && \
    apt-get install -y --no-install-recommends google-cloud-cli && \
    rm -rf /var/lib/apt/lists/*

# Install async-profiler
ARG ASYNC_PROFILER_VERSION=4.3
RUN mkdir -p /opt/profiler && \
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then AP_ARCH=arm64; else AP_ARCH=x64; fi && \
    curl -sL https://github.com/async-profiler/async-profiler/releases/download/v${ASYNC_PROFILER_VERSION}/async-profiler-${ASYNC_PROFILER_VERSION}-linux-${AP_ARCH}.tar.gz \
    | tar xz -C /opt/profiler --strip-components=1

# Copy HerdDB distribution
COPY opt/herddb /opt/herddb
RUN chmod 755 /opt/herddb/bin/*

# Copy log4j2 configuration for Docker
COPY log4j2.xml /opt/herddb/conf/log4j2.xml

ENV HERDDB_USE_ENV=true \
    herddb_env_http_enable=true \
    herddb_env_http_host=0.0.0.0 \
    herddb_env_server_host=0.0.0.0 \
    herddb_env_herddb_local_nodeid=local \
    HERDDB_HOME=/opt/herddb \
    LANG=en_US.UTF-8

WORKDIR /opt/herddb
EXPOSE 7000 9845 9846
ENTRYPOINT ["/opt/herddb/bin/service"]
CMD ["server", "console"]
