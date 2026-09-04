#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
deployment_dir="$(cd -- "${script_dir}/.." && pwd -P)"
compose=(docker compose --project-directory "${deployment_dir}" -f "${deployment_dir}/compose.yaml")
topic="${KAFKA_KERBEROS_TEST_TOPIC:-studio-kerberos-test}"
message="kerberos-smoke-$(date -u +%Y%m%d%H%M%S)"

"${compose[@]}" --profile tools up -d kerberos-client

client_exec=("${compose[@]}" exec -T kerberos-client)
client_config=/etc/kafka/client.properties
bootstrap=kafka.kerberos.test:19094

"${client_exec[@]}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${bootstrap}" \
    --command-config "${client_config}" \
    --create --if-not-exists --topic "${topic}" \
    --partitions 1 --replication-factor 1

printf '%s\n' "${message}" | "${client_exec[@]}" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "${bootstrap}" \
    --producer.config "${client_config}" \
    --topic "${topic}"

consumed="$("${client_exec[@]}" timeout 20 \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "${bootstrap}" \
    --consumer.config "${client_config}" \
    --topic "${topic}" --from-beginning \
    --max-messages 100 2>/dev/null || true)"

if ! grep -Fq "${message}" <<<"${consumed}"; then
    echo "Kerberos consumer did not receive the produced marker: ${message}" >&2
    exit 1
fi

if "${client_exec[@]}" timeout 12 \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${bootstrap}" \
    --list >/tmp/plain-kafka.out 2>/tmp/plain-kafka.err; then
    echo "Unauthenticated Kafka request unexpectedly succeeded" >&2
    exit 1
fi

echo "KERBEROS_KAFKA_SMOKE_OK topic=${topic} message=${message}"
echo "UNAUTHENTICATED_KAFKA_REQUEST_REJECTED"
