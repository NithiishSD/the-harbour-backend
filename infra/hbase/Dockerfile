FROM eclipse-temurin:17-jre-noble

ARG HBASE_VERSION=2.6.7
ARG MIRROR=https://downloads.apache.org/hbase

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && cd /tmp \
 && curl -fsSLO "${MIRROR}/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz" \
 && curl -fsSLO "${MIRROR}/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz.sha512" \
 && expected="$(cut -d: -f2- "hbase-${HBASE_VERSION}-bin.tar.gz.sha512" | tr -d ' \n' | tr 'A-F' 'a-f')" \
 && echo "${expected}  hbase-${HBASE_VERSION}-bin.tar.gz" | sha512sum -c - \
 && tar -xzf "hbase-${HBASE_VERSION}-bin.tar.gz" -C /opt \
 && mv "/opt/hbase-${HBASE_VERSION}" /opt/hbase \
 && rm -rf /opt/hbase/docs /tmp/hbase-* \
 && useradd --system --create-home hbase \
 && mkdir -p /data /opt/hbase/logs \
 && chown -R hbase:hbase /data /opt/hbase

COPY --chown=hbase:hbase hbase-site.xml /opt/hbase/conf/hbase-site.xml
COPY --chown=hbase:hbase start.sh /opt/hbase/start.sh

ENV HBASE_HEAPSIZE=1g \
    HBASE_REST_OPTS="-Xmx256m" \
    HBASE_MANAGES_ZK=true \
    PATH=/opt/hbase/bin:$PATH

USER hbase
VOLUME /data
EXPOSE 8080 16010
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=12 \
  CMD curl -fs http://localhost:8080/version/cluster || exit 1
CMD ["/bin/sh", "/opt/hbase/start.sh"]
