FROM debian:12-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -o Acquire::Retries=5 update \
    && apt-get -o Acquire::Retries=5 install -y --no-install-recommends \
        bash \
        ca-certificates \
        krb5-admin-server \
        krb5-kdc \
        krb5-user \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY krb5.conf /etc/krb5.conf
COPY kdc.conf /etc/krb5kdc/kdc.conf
COPY kdc-entrypoint.sh /usr/local/bin/kdc-entrypoint.sh

RUN chmod 0755 /usr/local/bin/kdc-entrypoint.sh \
    && mkdir -p /var/lib/krb5kdc /shared

EXPOSE 88/tcp 88/udp

ENTRYPOINT ["/usr/local/bin/kdc-entrypoint.sh"]
CMD ["run"]
